package streaming.dsl.mmlib.algs.feature

import org.apache.spark.SparkException
import java.{util => ju}
import org.apache.commons.lang3.math.NumberUtils
import org.apache.spark.sql.SparkSession
import streaming.dsl.mmlib.algs.MetaConst.QUANTILE_DISCRETIZAR_PATH
import streaming.dsl.mmlib.algs.meta.DiscretizerMeta

/**
 * Created by dxy_why on 2018/5/29.
 */
object DiscretizerFeature {
  val bucketizer = "bucketizer"
  val quantile = "quantile"
  private[feature] val SKIP_INVALID: String = "skip"
  private[feature] val ERROR_INVALID: String = "error"
  private[feature] val KEEP_INVALID: String = "keep"

  def getSplits(params: Map[String, String]): Array[Double] = {
    params.getOrElse("bucketSplits", "-inf,0.0,1.0,inf")
      .split(",").map(f => {
      f match {
        case "-inf" => Double.NegativeInfinity
        case "inf" => Double.PositiveInfinity
        case _ => NumberUtils.toDouble(f)
      }
    })
  }

  def getDiscretizerModel(spark: SparkSession, method: String, metaPath: String, trainParams: Map[String, String]): DiscretizerMeta = {
    method match {
      case bucketizer => getBucketizerModel(spark, metaPath, trainParams)
      case quantile => getQuantileModel(spark, metaPath, trainParams)
      case _ => DiscretizerMeta(trainParams, null)
    }
  }

  def getBucketizerModel(spark: SparkSession, metaPath: String, trainParams: Map[String, String]): DiscretizerMeta = {
    val handleInvalid = trainParams.getOrElse("handleInvalid", "keep")
    val keepInvalid = (handleInvalid == KEEP_INVALID)
    val splits = DiscretizerFeature.getSplits(trainParams)
    val transformer: Double => Double = features => binarySearchForBuckets(splits, features, keepInvalid)
    DiscretizerMeta(trainParams, transformer)
  }

  def getQuantileModel(spark: SparkSession, metaPath: String, trainParams: Map[String, String]): DiscretizerMeta = {
    import spark.implicits._
    val splits = spark.read.parquet(QUANTILE_DISCRETIZAR_PATH(metaPath, trainParams.getOrElse("inputCol", "")))
      .as[Double].collect()
    val handleInvalid = trainParams.getOrElse("handleInvalid", "keep")
    val keepInvalid = (handleInvalid == KEEP_INVALID)
    val transformer: Double => Double = features => binarySearchForBuckets(splits, features, keepInvalid)
    DiscretizerMeta(trainParams, transformer)
  }

  def binarySearchForBuckets(
                              splits: Array[Double],
                              feature: Double,
                              keepInvalid: Boolean): Double = {
    if (feature.isNaN) {
      if (keepInvalid) {
        splits.length - 1
      } else {
        throw new SparkException("Bucketizer encountered NaN value. To handle or skip NaNs," +
          " try setting Bucketizer.handleInvalid.")
      }
    } else if (feature == splits.last) {
      splits.length - 2
    } else {
      val idx = ju.Arrays.binarySearch(splits, feature)
      if (idx >= 0) {
        idx
      } else {
        val insertPos = -idx - 1
        if (insertPos == 0 || insertPos == splits.length) {
          throw new SparkException(s"Feature value $feature out of Bucketizer bounds" +
            s" [${splits.head}, ${splits.last}].  Check your features, or loosen " +
            s"the lower/upper bound constraints.")
        } else {
          insertPos - 1
        }
      }
    }
  }

}
