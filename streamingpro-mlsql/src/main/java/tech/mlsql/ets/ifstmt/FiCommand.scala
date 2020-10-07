package tech.mlsql.ets.ifstmt

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.IfContext
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.ets.BranchCommand

/**
 * 5/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class FiCommand(override val uid: String) extends SQLAlg with BranchCommand with WowParams with Logging with WowLog {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val ifContext = branchContext.pop().asInstanceOf[IfContext]

    if (ifContext.skipAll) {
      println(s"Skip FI :: ")
      return emptyDF
    }
    println(s"FI ::")
    ifContext.sqls.zipWithIndex.foreach { case (adaptor, index) =>
      println(s"execute: ${ifContext.ctxs(index).getText}")
      adaptor.parse(ifContext.ctxs(index))
    }
    emptyDF
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}
