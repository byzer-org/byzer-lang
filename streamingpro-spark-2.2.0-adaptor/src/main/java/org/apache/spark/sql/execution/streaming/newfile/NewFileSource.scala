package org.apache.spark.sql.execution.streaming.newfile

import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.streaming.{FileStreamSink, Sink}
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by allwefantasy on 22/5/2018.
  */
class DefaultSource extends StreamSinkProvider {
  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    val path = parameters.getOrElse("path", {
      throw new IllegalArgumentException("'path' is not specified")
    })
    if (outputMode != OutputMode.Append) {
      throw new AnalysisException(
        s"Data source ${getClass.getCanonicalName} does not support $outputMode output mode")
    }
    new NewFileStreamSink(sqlContext.sparkSession, parameters("path"), new ParquetFileFormat(), partitionColumns, parameters)
  }
}
