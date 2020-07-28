package tech.mlsql.autosuggest.meta
import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.util.EntityUtils
import tech.mlsql.common.utils.serder.json.JSONTool
/**
 * 26/7/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLEngineMetaProvider(url:String,owner:String) extends MetaProvider {
  override def search(key: MetaTableKey,extra: Map[String, String] = Map()): Option[MetaTable] = {
    val form = Form.form()

    var path = ""
    if (key.prefix.isDefined) {
       path += key.db.get+"."
    }

    path += key.table

    var sql = s"load ${key.prefix.get}.`${path}` as output;"
    form.add("sql",sql)
    form.add("owner",owner)
    val resp = Request.Post(url).bodyForm(form.build()).execute().returnResponse()
    if (resp.getStatusLine.getStatusCode == 200) {
      val metaTable = JSONTool.parseJson[MetaTable](EntityUtils.toString(resp.getEntity))
      Option(metaTable)
    } else None
  }

  override def list(extra: Map[String, String] = Map()): List[MetaTable] = {
    List()
  }
}
