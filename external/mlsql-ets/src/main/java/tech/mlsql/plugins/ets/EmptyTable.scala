package tech.mlsql.plugins.ets

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams

/**
 * 27/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class EmptyTable(override val uid: String) extends SQLAlg with WowParams {
  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.EmptyTable"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    df.sparkSession.emptyDataFrame
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}
