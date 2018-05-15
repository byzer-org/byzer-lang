package streaming.dsl.mmlib.algs

import java.util.UUID

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.feature.StringFeature
import org.apache.spark.sql.{functions => F}

/**
  * Created by allwefantasy on 7/5/2018.
  */
class SQLWord2VecInPlace extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    interval_train(df, params).write.mode(SaveMode.Overwrite).parquet(path)
  }

  def interval_train(df: DataFrame, params: Map[String, String]) = {
    val dicPaths = params.getOrElse("dicPaths", "")
    val inputCol = params.getOrElse("inputCol", "")
    val stopWordPath = params.getOrElse("stopWordPath", "")
    val flat = params.getOrElse("flatFeature", "true").toBoolean
    require(!inputCol.isEmpty, "inputCol is required when use SQLTfIdfInPlace")

    val mappingPath = if (params.contains("mappingPath")) params("mappingPath") else ("/tmp/" + UUID.randomUUID().toString)
    var newDF = StringFeature.word2vec(df, mappingPath, dicPaths, inputCol, stopWordPath)
    if (flat) {
      val flatFeatureUdf = F.udf((a: Seq[Seq[Double]]) => {
        a.flatMap(f => f)
      })
      newDF = newDF.withColumn(inputCol, flatFeatureUdf(F.col(inputCol)))
    }
    newDF
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}
