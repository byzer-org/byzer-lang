package streaming.dsl.mmlib

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Created by allwefantasy on 13/1/2018.
  */
trait SQLAlg {
  def train(df: DataFrame, path: String, params: Map[String, String]): Unit

  def load(sparkSession: SparkSession, path: String): Any

  def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction


}
