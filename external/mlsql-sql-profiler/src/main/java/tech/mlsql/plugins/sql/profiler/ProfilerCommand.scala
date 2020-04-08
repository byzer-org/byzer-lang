package tech.mlsql.plugins.sql.profiler

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, MLSQLUtils, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.common.utils.serder.json.JSONTool

/**
 * 27/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ProfilerCommand(override val uid: String) extends SQLAlg with WowParams {
  def this() = this(WowParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    import df.sparkSession.implicits._
    val args = JSONTool.parseJson[List[String]](params("parameters"))
    args match {
      case List("conf", left@_*) =>
        left match {
          case Seq("named", tableName) =>
            val newDF = df.sparkSession.conf.getAll.toSeq.toDF("name", "value")
            newDF.createOrReplaceTempView(tableName)
            newDF
          case _ => df.sparkSession.conf.getAll.toSeq.toDF("name", "value")
        }


      case List("sql", command) => df.sparkSession.sql(command)
      case List("explain", tableNameOrSQL, left@_*) =>
        val extended = left match {
          case Seq(extended) => extended.toBoolean
          case _ => true
        }

        val newDF = if (df.sparkSession.catalog.tableExists(tableNameOrSQL)) df.sparkSession.table(tableNameOrSQL)
        else df.sparkSession.sql(tableNameOrSQL)
        explain(newDF, extended)
    }
  }

  def explain(df: DataFrame, extended: Boolean) = {
    import df.sparkSession.implicits._
    val explain = MLSQLUtils.createExplainCommand(df.queryExecution.logical, extended = extended)
    val items = df.sparkSession.sessionState.executePlan(explain).executedPlan.executeCollect().
      map(_.getString(0)).mkString("\n")
    println(items)
    df.sparkSession.createDataset[Plan](Seq(Plan("doc", items))).toDF()
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}

case class Plan(name: String, info: String)
