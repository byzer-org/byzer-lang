package tech.mlsql

import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.util.EntityUtils
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.serder.json.JSONTool

/**
 * 7/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object ConsoleRequest {

  def execute(_params: Map[String, String]) = {
    val consoleUrl = ScriptSQLExec.context().userDefinedParam("__default__console_url__")
    val consoleSecret = ScriptSQLExec.context().userDefinedParam("__auth_secret__")
    val form = Form.form()

    val reqParams = JSONTool.parseJson[Map[String, String]](ScriptSQLExec.context().userDefinedParam("__PARAMS__"))
    reqParams.map { case (k, v) =>
      form.add("req." + k, v)
    }

    val params = _params ++ Map(
      "auth_secret" -> consoleSecret
    )

    params.foreach { case (k, v) =>
      form.add(k, v)
    }
    val resp = Request.Post(consoleUrl).connectTimeout(60 * 1000)
      .socketTimeout(10 * 60 * 1000).bodyForm(form.build()).execute().returnResponse()

    EntityUtils.toString(resp.getEntity)
  }

}
