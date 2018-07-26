package streaming.dsl.mmlib.algs

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{ChiSqSelector, DCT, PCA, PolynomialExpansion}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 26/7/2018.
  */
class SQLReduceFeaturesInPlace extends SQLAlg with MllibFunctions with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {

    val featureReduceType = params.getOrElse("featureReduceType", "pca")

    val model = featureReduceType.toLowerCase() match {
      case "pca" =>
        trainModels(df, path, params, () => {
          new PCA()
        })
      case "pe" =>
        trainModels(df, path, params, () => {
          new PolynomialExpansion()
        })
      case "dct" =>
        trainModels(df, path, params, () => {
          new DCT()
        })
      case "chisq" =>
        trainModels(df, path, params, () => {
          new ChiSqSelector()
        })
    }

    model.asInstanceOf[Transformer].transform(df).write.mode(SaveMode.Overwrite).parquet(path + "/data")

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}
