package streaming.dsl.mmlib.algs

import java.util.UUID

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 7/5/2018.
  */
class SQLTfIdfInPlace extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val dicPaths = params.getOrElse("dicPaths", "")
    val inputCol = params.getOrElse("inputCol", "")
    require(!inputCol.isEmpty, "inputCol is required when use SQLTfIdfInPlace")

    val mappingPath = "/tmp/" + UUID.randomUUID().toString
    val newDF = StringFeature.tfidf(df, mappingPath, dicPaths, inputCol, null, null)
    newDF.write.mode(SaveMode.Overwrite).parquet(path)
  }

  override def load(sparkSession: SparkSession, path: String): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = ???
}
