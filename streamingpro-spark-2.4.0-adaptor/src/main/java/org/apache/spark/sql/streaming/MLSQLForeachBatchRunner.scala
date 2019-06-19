package org.apache.spark.sql.streaming

import org.apache.spark.sql.{Dataset, Row, SparkSession}

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

  def run(dataStreamWriter: DataStreamWriter[Row], callback: (Dataset[Row], Long) => Unit): Unit = {
    dataStreamWriter.foreachBatch { (dataBatch, batchId) =>
      callback(dataBatch, batchId)
    }
  }
}
