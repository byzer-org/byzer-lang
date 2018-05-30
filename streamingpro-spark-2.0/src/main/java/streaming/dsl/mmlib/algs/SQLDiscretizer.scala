package streaming.dsl.mmlib.algs

import org.apache.spark.ml.feature.{Bucketizer, QuantileDiscretizer}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.MetaConst._
import streaming.dsl.mmlib.algs.feature.DiscretizerFeature
import streaming.dsl.mmlib.algs.meta.DiscretizerMeta

/**
 * Created by dxy_why on 2018/5/29.
 */
class SQLDiscretizer extends SQLAlg with Functions {

  def internal_train(df: DataFrame, params: Map[String, String]) = {
    val spark = df.sparkSession
    val path = params("path")
    val metaPath = getMetaPath(path)
    saveTraningParams(df.sparkSession, params, metaPath)
    val method = params.getOrElse("method", DiscretizerFeature.bucketizer)
    method match {
      case DiscretizerFeature.bucketizer =>
        val splits = DiscretizerFeature.getSplits(params)
        val bucketizer = new Bucketizer()
        bucketizer.setSplits(splits)
        configureModel(bucketizer, params)
        bucketizer.transform(df)

      case DiscretizerFeature.quantile =>
        val discretizer = new QuantileDiscretizer()
        configureModel(discretizer, params)
        val discretizerModel = discretizer.fit(df)
        val splits = discretizerModel.getSplits
        import spark.implicits._
        spark.createDataset(splits).write.mode(SaveMode.Overwrite).
          parquet(QUANTILE_DISCRETIZAR_PATH(metaPath, params.getOrElse("inputCol", "")))
        discretizerModel.transform(df)
    }
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val newDF = internal_train(df, params + ("path" -> path))
    newDF.write.mode(SaveMode.Overwrite).parquet(getDataPath(path))
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    val path = getMetaPath(_path)
    val (trainParams, df) = getTranningParams(spark, path)
    val method = trainParams.getOrElse("method", DiscretizerFeature.bucketizer)
    DiscretizerFeature.getDiscretizerModel(df.sparkSession, method, path, trainParams)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val meta = _model.asInstanceOf[DiscretizerMeta]
    val f = (value: Double) => {
      meta.discretizerFunc(value)
    }
    UserDefinedFunction(f, DoubleType, Some(Seq(DoubleType)))
  }
}
