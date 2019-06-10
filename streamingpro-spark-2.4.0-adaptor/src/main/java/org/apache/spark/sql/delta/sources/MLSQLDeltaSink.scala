package org.apache.spark.sql.delta.sources

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands.UpsertTableInDelta
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}


class MLSQLDeltaSink(
                      sqlContext: SQLContext,
                      path: Path,
                      partitionColumns: Seq[String],
                      outputMode: OutputMode,
                      options: DeltaOptions,
                      parameters: Map[String, String]
                    ) extends DeltaSink(
  sqlContext: SQLContext,
  path: Path,
  partitionColumns: Seq[String],
  outputMode: OutputMode,
  options: DeltaOptions) {

  private val deltaLog = DeltaLog.forTable(sqlContext.sparkSession, path)

  private val sqlConf = sqlContext.sparkSession.sessionState.conf

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val metadata = deltaLog.snapshot.metadata
    val readVersion = deltaLog.snapshot.version
    val isInitial = readVersion < 0
    if (!isInitial && parameters.contains(UpsertTableInDelta.ID_COLS)) {
      UpsertTableInDelta(data, None, Option(outputMode), deltaLog,
        new DeltaOptions(Map[String, String](), sqlContext.sparkSession.sessionState.conf),
        Seq(),
        Map(UpsertTableInDelta.ID_COLS -> parameters(UpsertTableInDelta.ID_COLS),
          UpsertTableInDelta.BATCH_ID -> batchId.toString
        )).run(sqlContext.sparkSession)

    } else {
      super.addBatch(batchId, data)
    }
  }
}
