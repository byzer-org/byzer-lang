package streaming.dsl.mmlib


import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Created by allwefantasy on 13/1/2018.
  */
trait SQLAlg {
  def train(df: DataFrame, path: String, params: Map[String, String]): Unit

  def load(path: String): Any

  def predict(_model: Any): UserDefinedFunction


}
