package tech.mlsql.autosuggest.app

import net.csdn.ServiceFramwork
import net.csdn.annotation.rest.At
import net.csdn.bootstrap.Application
import net.csdn.modules.http.RestRequest.Method
import net.csdn.modules.http.{ApplicationController, ViewType}
import org.apache.spark.sql.SparkSession
import streaming.dsl.ScriptSQLExec
import tech.mlsql.autosuggest.AutoSuggestContext
import tech.mlsql.autosuggest.meta.RestMetaProvider
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.common.utils.shell.command.ParamsUtil

/**
 * 11/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object Standalone extends {
  var sparkSession: SparkSession = null

  def main(args: Array[String]): Unit = {
    AutoSuggestContext.init
    val params = new ParamsUtil(args)
    val enableMLSQL = params.getParam("enableMLSQL", "false").toBoolean
    if (enableMLSQL) {
      sparkSession = SparkSession.builder().appName("local").master("local[*]").getOrCreate()
    }
    val applicationYamlName = params.getParam("config", "application.yml")

    ServiceFramwork.applicaionYamlName(applicationYamlName)
    ServiceFramwork.scanService.setLoader(classOf[Standalone])
    Application.main(args)

  }
}

class Standalone

class SuggestController extends ApplicationController {
  @At(path = Array("/run/script"), types = Array(Method.POST))
  def runScript = {

    if (param("executeMode", "") == "registerTable") {
      val prefix = paramOpt("prefix")
      val db = paramOpt("db")
      require(hasParam("table"), "table is required")
      require(hasParam("schema"), "schema is required")
      val table = param("table")
      val session = getSparkSession
      param("schemaType") match {
        case Constants.DB => session.createTableFromDBSQL(prefix, db, table, param("schema"))
        case Constants.HIVE => session.createTableFromHiveSQL(prefix, db, table, param("schema"))
        case Constants.JSON => session.createTableFromJson(prefix, db, table, param("schema"))
        case _ => session.createTableFromDBSQL(prefix, db, table, param("schema"))
      }
      render(200, JSONTool.toJsonStr(Map("success" -> true)))
    }
    val sql = param("sql")
    val lineNum = param("lineNum").toInt
    val columnNum = param("columnNum").toInt
    val isDebug = param("isDebug", "false").toBoolean
    val enableMemoryProvider = param("enableMemoryProvider", "true").toBoolean

    val context = new AutoSuggestContext(Standalone.sparkSession,
      AutoSuggestController.mlsqlLexer,
      AutoSuggestController.sqlLexer)
    context.setDebugMode(isDebug)

    if (enableMemoryProvider) {
      context.setUserDefinedMetaProvider(AutoSuggestContext.memoryMetaProvider)
    }

    val searchUrl = paramOpt("searchUrl")
    val listUrl = paramOpt("listUrl")

    (searchUrl, listUrl) match {
      case (Some(searchUrl), Some(listUrl)) =>
        context.setUserDefinedMetaProvider(new RestMetaProvider(searchUrl, listUrl))
      case (None, None) =>
    }

    render(200, JSONTool.toJsonStr(context.buildFromString(sql).suggest(lineNum, columnNum)), ViewType.string)
  }

  def paramOpt(name: String) = {
    if (hasParam(name)) Option(param(name)) else None
  }

  def getSparkSession = {
    val session = if (ScriptSQLExec.context() != null) ScriptSQLExec.context().execListener.sparkSession else Standalone.sparkSession
    new SchemaRegistry(session)
  }
}
