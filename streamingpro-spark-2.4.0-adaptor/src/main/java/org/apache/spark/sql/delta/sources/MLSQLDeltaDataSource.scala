package org.apache.spark.sql.delta.sources

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.commands.{UpsertTableInDelta, WriteIntoDelta}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOptions}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * 2019-06-10 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLDeltaDataSource extends DeltaDataSource {
  override def createSink(
                           sqlContext: SQLContext,
                           parameters: Map[String, String],
                           partitionColumns: Seq[String],
                           outputMode: OutputMode): Sink = {
    val path = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })
    if (outputMode != OutputMode.Append && outputMode != OutputMode.Complete) {
      throw DeltaErrors.outputModeNotSupportedException(getClass.getName, outputMode)
    }
    val deltaOptions = new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf)
    new MLSQLDeltaSink(sqlContext, new Path(path), partitionColumns, outputMode, deltaOptions, parameters)
  }

  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               data: DataFrame): BaseRelation = {
    val path = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })
    val partitionColumns = parameters.get(DeltaSourceUtils.PARTITIONING_COLUMNS_KEY)
      .map(DeltaDataSource.decodePartitioningColumns)
      .getOrElse(Nil)

    val deltaLog = DeltaLog.forTable(sqlContext.sparkSession, path)

    if (parameters.contains(UpsertTableInDelta.ID_COLS)) {
      UpsertTableInDelta(data, Option(mode), None, deltaLog,
        new DeltaOptions(Map[String, String](), sqlContext.sparkSession.sessionState.conf),
        Seq(),
        parameters)

    } else {
      WriteIntoDelta(
        deltaLog = deltaLog,
        mode = mode,
        new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf),
        partitionColumns = partitionColumns,
        configuration = Map.empty,
        data = data).run(sqlContext.sparkSession)
    }

    deltaLog.createRelation()
  }
}
