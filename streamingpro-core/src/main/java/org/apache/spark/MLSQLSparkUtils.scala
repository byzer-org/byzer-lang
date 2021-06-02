package org.apache.spark

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.BinaryType

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


  def isFileTypeTable(df: DataFrame): Boolean = {
    if (df.schema.fields.length != 3) return false
    val fields = df.schema.fields
    fields(0).name == "start" && fields(1).name == "offset" && fields(2).name == "value" && fields(2).dataType == BinaryType
  }
}
