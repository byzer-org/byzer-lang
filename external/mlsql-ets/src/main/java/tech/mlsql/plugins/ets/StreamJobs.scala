package tech.mlsql.plugins.ets

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams

/**
 * 14/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class StreamJobs (override val uid: String) extends SQLAlg with WowParams {
  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.StreamJobs"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val names = df.sparkSession.streams.active.map(item=>item.name)
    import df.sparkSession.implicits._
    df.sparkSession.createDataset[String](names).toDF("values")
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}
