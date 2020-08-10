package tech.mlsql.plugins.ets

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
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
    val groupBys = params("columnLeft").split(",")
    var temp = df.groupBy(groupBys.head, groupBys.drop(1): _*)

    if (params.get("columnHeaderFields").isDefined) {
      val columnHeaderFields = params("columnHeaderFields").split(",")
      temp = temp.pivot(params("columnHeader"), columnHeaderFields)
    } else {
      temp = temp.pivot(params("columnHeader"))
    }


    val aggs = params("sunFunc").split(",").map(item => F.expr(item).as(s"""${params("columnSum")}_${item.split("\\(").head}"""))
    temp.agg(aggs.head, aggs.drop(1): _*)

  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    List()
  }

  final val columnLeft: Param[String] = new Param[String](this, "columnLeft",
    """
      |
      |group by columns in pivot table.
      |separated by comma.
      |""".stripMargin)
  final val columnHeaderFields: Param[String] = new Param[String](this, "columnHeaderFields",
    """
      |If there are too much values in  columnHeader, you can use this parameter
      |to narrow the values.
      |separated by comma.
      |""".stripMargin)
  final val columnHeader: Param[String] = new Param[String](this, "columnHeader",
    """
      |values in this column will be treated as new columns.
      |""".stripMargin)
  final val sunFunc: Param[String] = new Param[String](this, "sunFunc",
    """
      |agg functions, max,min,avg,sum
      |separated by comma.
      |""".stripMargin)
}
