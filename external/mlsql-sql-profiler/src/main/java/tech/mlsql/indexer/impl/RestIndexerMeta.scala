package tech.mlsql.indexer.impl

import java.nio.charset.Charset

import org.apache.http.client.fluent.{Form, Request}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.indexer.{MLSQLIndexerMeta, MlsqlIndexerItem, MlsqlOriTable}

/**
 * 21/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class RestIndexerMeta(url: String, token: String,timeout:Int=2000) extends MLSQLIndexerMeta {
  override def fetchIndexers(tableNames: List[MlsqlOriTable], options: Map[String, String]): Map[MlsqlOriTable, List[MlsqlIndexerItem]] = {
    val form = Form.form()
    form.add("data", JSONTool.toJsonStr(tableNames))
    form.add("auth_secret", token)
    try {
      val resp = Request.Post(url.stripSuffix("/")+"/"+"/indexers")
        .connectTimeout(timeout)
        .socketTimeout(timeout).bodyForm(form.build())
        .execute()
      val value = resp.returnContent().asString(Charset.forName("UTF-8"))
      JSONTool.parseJson[Map[MlsqlOriTable, List[MlsqlIndexerItem]]](value)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Map()
    }

  }

  override def registerIndexer(indexer: MlsqlIndexerItem): Unit = {
    val form = Form.form()
    form.add("data", JSONTool.toJsonStr(indexer))
    form.add("auth_secret", token)
    val resp = Request.Post(url.stripSuffix("/")+"/"+"/indexers/register")
      .connectTimeout(timeout)
      .socketTimeout(timeout).bodyForm(form.build())
      .execute()
    val status = resp.returnResponse().getStatusLine.getStatusCode
    if (status != 200) {
      throw new RuntimeException(resp.returnContent().asString(Charset.forName("UTF-8")))
    }
  }
}
