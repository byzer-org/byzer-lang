package streaming.dsl.mmlib.algs.feature

import java.util.UUID

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, functions => F}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.algs.MetaConst._
import streaming.dsl.mmlib.algs.meta.OutlierValueMeta


/**
  * Created by allwefantasy on 15/5/2018.
  */
trait BaseFeatureFunctions {

  def replaceColumn(newDF: DataFrame, inputCol: String, udf: UserDefinedFunction) = {
    //newDF.withColumn(inputCol + "_tmp", udf(F.col(inputCol))).drop(inputCol).withColumnRenamed(inputCol + "_tmp", inputCol)
    newDF.withColumn(inputCol, udf(F.col(inputCol)))
  }

  def killSingleColumnOutlierValue(df: DataFrame, field: String) = {
    val quantiles = df.stat.approxQuantile(field, Array(0.25, 0.5, 0.75), 0.0)
    val Q1 = quantiles(0)
    val Q3 = quantiles(2)
    val IQR = Q3 - Q1
    val lowerRange = Q1 - 1.5 * IQR
    val upperRange = Q3 + 1.5 * IQR

    val Q2 = quantiles(1)
    //df.filter(s"value < $lowerRange or value > $upperRange")
    val udf = F.udf((a: Double) => {
      if (a < lowerRange || a > upperRange) {
        Q2
      } else a
    })

    val newDF = df.withColumn(field, udf(F.col(field)))
    (newDF, OutlierValueMeta(field, lowerRange, upperRange, Q2))
  }

  def asBreeze(vector: Vector) = {
    vector match {
      case v: DenseVector => new BDV(v.values)
      case v: SparseVector => new BDV(v.values)
    }

  }

  def getTempCol = {
    "_features_" + UUID.randomUUID().toString.replace("_", "").replace("-", "")
  }

  def getFieldGroupName(fields: Seq[String]) = {
    fields.mkString("_")
  }

  def expandColumnsFromVector(df: DataFrame, fields: Seq[String], vectorField: String) = {
    var newDF = df
    fields.zipWithIndex.foreach { f =>
      val (value, index) = f
      val func = F.udf((v: Vector) => {
        v.toDense.values(index)
      })
      newDF = newDF.withColumn(value, func(F.col(vectorField)))
    }
    newDF.drop(vectorField)
  }
}
