package streaming.dsl.mmlib.algs

import org.apache.spark.ml.feature.{Bucketizer, DiscretizerFeature, QuantileDiscretizer}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.MetaConst._
import streaming.dsl.mmlib.algs.meta.DiscretizerMeta

/**
 * Created by dxy_why on 2018/5/29.
 */
class SQLDiscretizer extends SQLAlg with Functions {

  def internal_train(df: DataFrame, params: Map[String, String]) = {
    val spark = df.sparkSession
    import spark.implicits._
    val path = params("path")
    val metaPath = getMetaPath(path)
    val method = params.getOrElse(DiscretizerParamsConstrant.METHOD, DiscretizerFeature.BUCKETIZER_METHOD)
    saveTraningParams(df.sparkSession, params, metaPath)

    val fitParamsWithIndex = arrayParamsWithIndex(DiscretizerParamsConstrant.PARAMS_PREFIX, params)
    require(fitParamsWithIndex.size > 0, "fitParams should be configured")

    // we need save metadatas with index, because we need index
    val metas: Array[(Int, DiscretizerTrainData)] =
      method match {
        case DiscretizerFeature.BUCKETIZER_METHOD =>
          fitParamsWithIndex.map {
            case (index, map) =>
              val splitArray = DiscretizerFeature.getSplits(
                map.getOrElse(DiscretizerParamsConstrant.SPLIT_ARRAY, "")
              )
              (index, DiscretizerFeature.parseParams(map, splitArray))
          }

        case DiscretizerFeature.QUANTILE_METHOD =>
          fitParamsWithIndex.map {
            case (index, map) =>
              val discretizer = new QuantileDiscretizer()
              configureModel(discretizer, map)
              val discretizerModel = discretizer.fit(df)
              val splits = discretizerModel.getSplits
              (index, DiscretizerFeature.parseParams(map, splits))
          }
    }
    spark.createDataset(metas).write.mode(SaveMode.Overwrite).
      parquet(DISCRETIZER_PATH(metaPath))
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    internal_train(df, params + ("path" -> path))
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    import spark.implicits._
    val path = getMetaPath(_path)

    val metas = spark.read
      .parquet(DISCRETIZER_PATH(path))
      .as[(Int, DiscretizerTrainData)]
      .collect()
      .sortBy(_._1)
      .map(_._2)

    val func = DiscretizerFeature.getDiscretizerPredictFun(spark, metas)
    DiscretizerMeta(metas, func)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val meta = _model.asInstanceOf[DiscretizerMeta]
    UserDefinedFunction(meta.discretizerFunc, ArrayType(DoubleType), Some(Seq(ArrayType(DoubleType))))
  }
}

case class DiscretizerTrainData(
    inputCol: String,
    splits: Array[Double],
    handleInvalid: Boolean,
    params: Map[String, String])

object DiscretizerParamsConstrant {
  /**
   * 参数数组前缀
   */
  val PARAMS_PREFIX = "fitParam"
  /**
   * 输入列名
   */
  val INPUT_COLUMN = "inputCol"
  /**
   * split参数数组
   */
  val SPLIT_ARRAY = "splitArray"
  /**
   * 散列化的方法，支持：bucketizer,quantile
   */
  val METHOD = "method"
  /**
   *
   */
  val HANDLE_INVALID = "handleInvalid"
}
