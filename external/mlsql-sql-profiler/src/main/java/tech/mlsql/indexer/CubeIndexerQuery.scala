package tech.mlsql.indexer

import com.alibaba.sparkcube.optimizer.GenPlanFromCache
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{CubeSharedState, DataFrame, DataSetHelper, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams

/**
 * 25/1/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class CubeIndexerQuery(override val uid: String) extends SQLAlg with WowParams {

  def this() = this(WowParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val session = df.sparkSession
    val rule = new GenPlanFromCache(session)
    val finalLP = rule.apply(session.sql(params("sql")).queryExecution.analyzed)
    println(finalLP)
    val sparkSession = ScriptSQLExec.context().execListener.sparkSession
    val ds = DataSetHelper.create(sparkSession, finalLP)
    ds
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}
