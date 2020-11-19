package org.apache.spark

import org.apache.spark.deploy.SparkHadoopUtil

/**
 * 2019-08-16 WilliamZhu(allwefantasy@gmail.com)
 */
object MLSQLSparkUtils {
  def rpcEnv() = {
    SparkEnv.get.rpcEnv
  }

  def blockManager = {
    SparkEnv.get.blockManager
  }

  def sparkHadoopUtil = {
    SparkHadoopUtil.get
  }

}
