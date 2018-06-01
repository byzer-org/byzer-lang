package org.apache.spark.ml.feature

import org.apache.commons.lang3.math.NumberUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import streaming.dsl.mmlib.algs.{DiscretizerParamsConstrant, DiscretizerTrainData}

/**
 * Created by dxy_why on 2018/5/29.
 */
object DiscretizerFeature {
  val BUCKETIZER_METHOD = "bucketizer"
  val QUANTILE_METHOD = "quantile"
  private[feature] val SKIP_INVALID: String = "skip"
  private[feature] val ERROR_INVALID: String = "error"
  private[feature] val KEEP_INVALID: String = "keep"

  def parseParams(params: Map[String, String], splits: Array[Double]): DiscretizerTrainData = {
    val handleInvalid = params.getOrElse(DiscretizerParamsConstrant.HANDLE_INVALID, "keep") == Bucketizer.KEEP_INVALID
    val inputCol = params.getOrElse(DiscretizerParamsConstrant.INPUT_COLUMN, null)
    require(inputCol != null, "inputCol should be configured.")
    DiscretizerTrainData(inputCol, splits, handleInvalid, params)
  }

  def getSplits(params: Map[String, String]): Array[Double] = {
    params.getOrElse("splitArray", "-inf,0.0,1.0,inf")
      .split(",").map(f => {
      f match {
        case "-inf" => Double.NegativeInfinity
        case "inf" => Double.PositiveInfinity
        case _ => NumberUtils.toDouble(f)
      }
    })
  }

  def getSplits(arrayString: String): Array[Double] = {
    arrayString.split(",").map(f => {
      f match {
        case "-inf" => Double.NegativeInfinity
        case "inf" => Double.PositiveInfinity
        case _ => NumberUtils.toDouble(f)
      }
    })
  }

  def getDiscretizerPredictFun(spark: SparkSession, metas: Array[DiscretizerTrainData]): Seq[Double] => Seq[Double] = {

    val metasbc = spark.sparkContext.broadcast(metas)
    val transformer: Seq[Double] => Seq[Double] = features => {
      features.zipWithIndex.map {
        case (feature, index) =>
          val meta = metasbc.value(index)
          Bucketizer.binarySearchForBuckets(meta.splits, feature, meta.handleInvalid)
      }
    }
    transformer

  }

}
