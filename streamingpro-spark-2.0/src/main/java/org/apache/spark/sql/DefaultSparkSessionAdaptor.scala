package org.apache.spark.sql

import org.apache.spark.SparkConf

/**
 * 11/21/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SparkSessionAdaptor(conf: SparkConf) {
  val sparkSession = SparkSession.builder().config(conf).getOrCreate()

  def readStream = {
    sparkSession.readStream
  }

  def sqlContext = {
    sparkSession.sqlContext
  }
}
