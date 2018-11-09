package org.apache.spark.sql.execution.streaming.newfile

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormat, FileFormatWriter, WriteJobStatsTracker}
import org.apache.spark.sql.execution.streaming.{FileStreamSink, FileStreamSinkLog, ManifestFileCommitProtocol, Sink}
import org.apache.spark.sql.{SparkSession, _}
import _root_.streaming.common.RenderEngine
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FileFormatWriter.OutputSpec
import org.apache.spark.util.SerializableConfiguration
import org.joda.time.DateTime

class NewFileStreamSink(
                         sparkSession: SparkSession,
                         _path: String,
                         fileFormat: FileFormat,
                         partitionColumnNames: Seq[String],
                         options: Map[String, String]) extends Sink with Logging {

  def evaluate(value: String, context: Map[String, AnyRef]) = {
    RenderEngine.render(value, context)
  }

  def path = {
    evaluate(_path, Map("path_date" -> new DateTime()))
  }

  private def basePath = new Path(path)

  private def logPath = new Path(basePath, FileStreamSink.metadataDir)

  private def fileLog =
    new FileStreamSinkLog(FileStreamSinkLog.VERSION, sparkSession, logPath.toUri.toString)

  private def basicWriteJobStatsTracker: BasicWriteJobStatsTracker = {
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    new BasicWriteJobStatsTracker(serializableHadoopConf, BasicWriteJobStatsTracker.metrics)
  }
  private val hadoopConf = sparkSession.sessionState.newHadoopConf()

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= fileLog.getLatest().map(_._1).getOrElse(-1L)) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val committer = FileCommitProtocol.instantiate(
        className = sparkSession.sessionState.conf.streamingFileCommitProtocolClass,
        jobId = batchId.toString,
        outputPath = path)

      committer match {
        case manifestCommitter: ManifestFileCommitProtocol =>
          manifestCommitter.setupManifestOptions(fileLog, batchId)
        case _ => // Do nothing
      }

      // Get the actual partition columns as attributes after matching them by name with
      // the given columns names.
      val partitionColumns: Seq[Attribute] = partitionColumnNames.map { col =>
        val nameEquality = data.sparkSession.sessionState.conf.resolver
        data.logicalPlan.output.find(f => nameEquality(f.name, col)).getOrElse {
          throw new RuntimeException(s"Partition column $col not found in schema ${data.schema}")
        }
      }
      val qe = data.queryExecution

      FileFormatWriter.write(
        sparkSession = sparkSession,
        plan = qe.executedPlan,
        fileFormat = fileFormat,
        committer = committer,
        outputSpec = FileFormatWriter.OutputSpec(path, Map.empty, qe.analyzed.output),
        hadoopConf = hadoopConf,
        partitionColumns = partitionColumns,
        bucketSpec = None,
        statsTrackers = Seq(basicWriteJobStatsTracker),
        options = options)
    }
  }

  override def toString: String = s"FileSink[$path]"
}
