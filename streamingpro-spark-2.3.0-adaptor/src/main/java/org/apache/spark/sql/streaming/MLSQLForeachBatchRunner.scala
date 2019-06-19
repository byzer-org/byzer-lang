package org.apache.spark.sql.streaming

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * 2019-05-26 WilliamZhu(allwefantasy@gmail.com)
  */
object MLSQLForeachBatchRunner {
  def run(dataStreamWriter: DataStreamWriter[Row], outputName: String, callback: (Long, SparkSession) => Unit): Unit = {
    throw new RuntimeException("Spark 2.3.x does not support `Stream Sub Batch Query`")
  }

  def run(dataStreamWriter: DataStreamWriter[Row], callback: (Dataset[Row], Long) => Unit): Unit = {
    throw new RuntimeException("Spark 2.3.x does not support `Stream Sub Batch Query`")
  }
}
