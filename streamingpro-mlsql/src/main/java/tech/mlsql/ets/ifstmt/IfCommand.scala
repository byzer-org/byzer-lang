package tech.mlsql.ets.ifstmt

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.dsl.{ForContext, IfContext}
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.BranchCommand

/**
 * 5/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class IfCommand(override val uid: String) extends SQLAlg with BranchCommand with WowParams with Logging with WowLog {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val skipThisIf = if (!branchContext.isEmpty) {
      branchContext.top match {
        case a: IfContext =>
          !a.shouldExecute
        case a: ForContext => throw new MLSQLException("For is not support yet")
      }
    } else false

    ifContextInit
    if (skipThisIf) {
      val newIfContext = branchContext.pop().asInstanceOf[IfContext]
      branchContext.push(newIfContext.copy(skipAll = true))

      if(traceBC){
        pushTrace(s"Skip If :: ${params}")
      }

      return emptyDF

    }
    val args = JSONTool.parseJson[List[String]](params("parameters"))
    val command = args.mkString(" ")
    val _ifContext = branchContext.pop().asInstanceOf[IfContext]
    val (conditionValue,ifContext) = evaluateIfElse(_ifContext,command,params)

    if(traceBC){
      pushTrace(s"If :: ${params} :: ${conditionValue}")
    }
    val newIfContext = ifContext.copy(
      shouldExecute = conditionValue,
      haveMatched = conditionValue)
    branchContext.push(newIfContext)
    emptyDF

  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}
