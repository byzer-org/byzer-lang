package tech.mlsql.plugins.ets

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.dsl.scope.ParameterVisibility.ParameterVisibility
import tech.mlsql.dsl.scope.{ParameterVisibility, SetVisibilityParameter}

import scala.collection.mutable

/**
 * 31/8/2020 WilliamZhu(allwefantasy@gmail.com)
 * !lastTableName;
 */
class LastTableName(override val uid: String) extends SQLAlg with DslTool with ETAuth with WowParams {
  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.LastTableName"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getLastSelectTable() match {
      case Some(tableName) =>
        context.execListener.addEnv("__last_table_name__", tableName)
        val parameterVisibility = new mutable.HashSet[ParameterVisibility]
        parameterVisibility.add(ParameterVisibility.ALL)
        context.execListener.addEnvVisibility("__last_table_name__", SetVisibilityParameter(tableName, parameterVisibility))
      case None => throw new MLSQLException("!lastTableName cannot found table")
    }
    df.sparkSession.emptyDataFrame

  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    List()
  }

  final val filePath: Param[String] = new Param[String](this, "filePath",
    """
      |file name
      |""".stripMargin)

}