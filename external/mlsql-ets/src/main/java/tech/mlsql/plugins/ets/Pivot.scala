package tech.mlsql.plugins.ets

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

/**
 * 8/8/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class Pivot(override val uid: String) extends SQLAlg with ETAuth with WowParams {
  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.pivot"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val temp = df
      .groupBy(params("columnLeft"))
      .pivot(params("columnHeader"))

    params("sunFunc") match {
      case "sum" => temp.sum(params("columnSum"))
      case "count" => temp.count()
    }

  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = ???
}
