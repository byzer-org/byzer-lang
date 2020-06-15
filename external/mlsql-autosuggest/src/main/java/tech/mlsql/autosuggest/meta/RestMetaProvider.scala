package tech.mlsql.autosuggest.meta

import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.util.EntityUtils
import tech.mlsql.common.utils.serder.json.JSONTool

/**
 * 15/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class RestMetaProvider(searchUrl: String, listUrl: String) extends MetaProvider {
  override def search(key: MetaTableKey): Option[MetaTable] = {
    val form = Form.form()
    if (key.prefix.isDefined) {
      form.add("prefix", key.prefix.get)
    }
    if (key.db.isDefined) {
      form.add("db", key.db.get)
    }
    form.add("table", key.table)
    val resp = Request.Post(searchUrl).bodyForm(form.build()).execute().returnResponse()
    if (resp.getStatusLine.getStatusCode == 200) {
      val metaTable = JSONTool.parseJson[MetaTable](EntityUtils.toString(resp.getEntity))
      Option(metaTable)
    } else None
  }

  override def list: List[MetaTable] = {

    val resp = Request.Post(listUrl).execute().returnResponse()
    if (resp.getStatusLine.getStatusCode == 200) {
      val metaTables = JSONTool.parseJson[List[MetaTable]](EntityUtils.toString(resp.getEntity))
      metaTables
    } else List()
  }
}
