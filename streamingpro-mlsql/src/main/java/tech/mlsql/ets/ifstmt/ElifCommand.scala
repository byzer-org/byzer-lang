package tech.mlsql.ets.ifstmt

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.IfContext
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.BranchCommand

/**
 * 5/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ElifCommand(override val uid: String) extends SQLAlg with BranchCommand with WowParams with Logging with WowLog {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val _ifContext = branchContext.pop().asInstanceOf[IfContext]
    if (_ifContext.skipAll) {
      branchContext.push(_ifContext)

      if(traceBC){
        pushTrace(s"Skip Elif :: ${params}")
      }
      
      return emptyDF
    }

    if (_ifContext.haveMatched) {
      val newIfContext = _ifContext.copy(shouldExecute = false)
      branchContext.push(newIfContext)
      return emptyDF
    }

    val args = JSONTool.parseJson[List[String]](params("parameters"))
    val command = args.mkString(" ")
    val (conditionValue,ifContext) = evaluateIfElse(_ifContext,command,params)
    if(traceBC){
      pushTrace(s"Elif :: ${params} :: ${conditionValue}")
    }
    val newIfContext = ifContext.copy(shouldExecute = conditionValue, haveMatched = conditionValue)
    branchContext.push(newIfContext)
    emptyDF

  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}
