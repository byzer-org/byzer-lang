package tech.mlsql.runtime

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 2019-09-09 WilliamZhu(allwefantasy@gmail.com)
  */
trait MLSQLRuntimeLifecycle {
  def beforeRuntimeStarted(params: Map[String, String], conf: SparkConf): Unit

  def afterRuntimeStarted(params: Map[String, String], conf: SparkConf, rootSparkSession: SparkSession): Unit

}

