package tech.mlsql.autosuggest.app

import net.csdn.ServiceFramwork
import net.csdn.annotation.rest.At
import net.csdn.bootstrap.Application
import net.csdn.modules.http.RestRequest.Method
import net.csdn.modules.http.{ApplicationController, ViewType}
import tech.mlsql.autosuggest.AutoSuggestContext
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.common.utils.shell.command.ParamsUtil

/**
 * 11/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object Standalone extends {
  def main(args: Array[String]): Unit = {
    AutoSuggestContext.init
    val params = new ParamsUtil(args)
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
    val sql = param("sql")
    val lineNum = param("lineNum").toInt
    val columnNum = param("columnNum").toInt
    val isDebug = param("isDebug", "false").toBoolean

    val context = new AutoSuggestContext(null,
      AutoSuggestController.mlsqlLexer,
      AutoSuggestController.sqlLexer)
    context.setDebugMode(isDebug)

    render(200, JSONTool.toJsonStr(context.buildFromString(sql).suggest(lineNum, columnNum)), ViewType.string)
  }
}
