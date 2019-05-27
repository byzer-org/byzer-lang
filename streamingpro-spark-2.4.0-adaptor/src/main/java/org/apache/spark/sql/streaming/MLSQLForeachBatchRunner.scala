package org.apache.spark.sql.streaming

import org.apache.spark.sql.{Row, SparkSession}

/**
  * 2019-05-26 WilliamZhu(allwefantasy@gmail.com)
  */
object MLSQLForeachBatchRunner {
  def run(dataStreamWriter: DataStreamWriter[Row], outputName: String, callback: (Long, SparkSession) => Unit): Unit = {
    dataStreamWriter.foreachBatch { (dataBatch, batchId) =>
      dataBatch.createOrReplaceTempView(outputName)
      callback(batchId, dataBatch.sparkSession)
    }
  }
}
