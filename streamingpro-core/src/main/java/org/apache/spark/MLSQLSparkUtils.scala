package org.apache.spark

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.mllib.linalg.Vector
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

  def transformDense(
                      idf: Vector,
                      values: Array[Double]): Array[Double] = {
    val n = values.length
    val newValues = new Array[Double](n)
    var j = 0
    while (j < n) {
      newValues(j) = values(j) * idf(j)
      j += 1
    }
    newValues
  }

  def transformSparse(
                       idf: Vector,
                       indices: Array[Int],
                       values: Array[Double]): (Array[Int], Array[Double]) = {
    val nnz = indices.length
    val newValues = new Array[Double](nnz)
    var k = 0
    while (k < nnz) {
      newValues(k) = values(k) * idf(indices(k))
      k += 1
    }
    (indices, newValues)
  }


  def isFileTypeTable(df: DataFrame): Boolean = {
    if (df.schema.fields.length != 3) return false
    val fields = df.schema.fields
    fields(0).name == "start" && fields(1).name == "offset" && fields(2).name == "value" && fields(2).dataType == BinaryType
  }
}
