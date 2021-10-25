package tech.mlsql.ets

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.form.{Extra, FormParams, Text}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.version.VersionCompatibility


class RunScript(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams with ETAuth {
  def this() = this(BaseParams.randomUID())

  // 
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val context = ScriptSQLExec.context()
    val command = JSONTool.parseJson[List[String]](params("parameters")).toArray
    val sparkOpt = Option(df.sparkSession)
    command match {
      case Array(script, "named", tableName) =>
        var jobRes: DataFrame = ScriptRunner.rubSubJob(
          script,
          (_df: DataFrame) => {},
          sparkOpt,
          true,
          true).get
        jobRes.createOrReplaceTempView(tableName)
        jobRes
      case _ => throw new RuntimeException("try !runScript code named table1")
    }

  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    List()
  }

  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
  }


  override def explainParams(sparkSession: SparkSession): DataFrame = _explainParams(sparkSession)

  final val parameters: Param[String]  = new Param[String] (this, "parameters",
    FormParams.toJson(Text(
      name = "parameters",
      value = "",
      extra = Extra(
        doc =
          """
            | Required. The RunScripts parameters.
          """,
        label = "The RunScripts parameters.",
        options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )
  setDefault(parameters, "")

  override def doc: Doc = Doc(MarkDownDoc,
    s"""
       |When you want to get the result from command and used
       | in next command(SQL), you can use !last command.
       |
       |For example:
       |
       |```
       |${codeExample.code}
       |```
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode,
    """
      |!runScript ''' select 1 as a as b; ''' named output;
    """.stripMargin)

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???


}
