package tech.mlsql.autosuggest.app

import net.csdn.ServiceFramwork
import net.csdn.annotation.rest.At
import net.csdn.bootstrap.Application
import net.csdn.modules.http.RestRequest.Method
import net.csdn.modules.http.{ApplicationController, ViewType}
import org.apache.spark.sql.SparkSession
import streaming.dsl.ScriptSQLExec
import tech.mlsql.autosuggest.AutoSuggestContext
import tech.mlsql.common.utils.shell.command.ParamsUtil

import scala.collection.JavaConverters._

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
      val respStr = new RegisterTableController().run(params().asScala.toMap)
      render(200, respStr, ViewType.json)

    }
    if (param("executeMode", "") == "sqlFunctions") {
      val respStr = new SQLFunctionController().run(params().asScala.toMap)
      render(200, respStr, ViewType.json)

    }
    val respStr = new AutoSuggestController().run(params.asScala.toMap)
    render(200, respStr, ViewType.json)
  }
  
}
