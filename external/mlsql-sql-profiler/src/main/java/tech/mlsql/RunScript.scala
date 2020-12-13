package tech.mlsql

import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.util.EntityUtils
import streaming.core.strategy.platform.PlatformManager
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.serder.json.JSONTool

/**
 * 7/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object RunScript {

  def execute(url: String, jobName: String, sql: String) = {
    var params = JSONTool.parseJson[Map[String, String]](ScriptSQLExec.context().userDefinedParam("__PARAMS__"))
    params = params ++ Map("sql" -> sql, "jobName" -> jobName)
    val form = Form.form()
    params.foreach { case (k, v) =>
      form.add(k, v)
    }
    val resp = Request.Post(url).connectTimeout(60 * 1000)
      .socketTimeout(10 * 60 * 1000).bodyForm(form.build()).execute().returnResponse()
    EntityUtils.toString(resp.getEntity)
  }

  def getLocalUrl = {
    val runtime = PlatformManager.getRuntime
    val port = runtime.params.getOrDefault("streaming.driver.port", "9003").toString
    s"http://127.0.0.1:${port}"
  }

}
