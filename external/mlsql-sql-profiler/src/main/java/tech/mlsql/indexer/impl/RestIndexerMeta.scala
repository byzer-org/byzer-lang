package tech.mlsql.indexer.impl

import java.nio.charset.Charset

import org.apache.http.client.fluent.{Form, Request}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.indexer.{MLSQLIndexerMeta, MlsqlIndexer, MlsqlOriTable}

/**
 * 21/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class RestIndexerMeta(url: String, token: String) extends MLSQLIndexerMeta {
  override def fetchIndexers(tableNames: List[MlsqlOriTable], options: Map[String, String]): Map[MlsqlOriTable, List[MlsqlIndexer]] = {
    val form = Form.form()
    form.add("data", JSONTool.toJsonStr(tableNames))
    form.add("auth_secret", token)
    try {
      val resp = Request.Post(url)
        .connectTimeout(2000)
        .socketTimeout(2000).bodyForm(form.build())
        .execute()
      val value = resp.returnContent().asString(Charset.forName("UTF-8"))
      JSONTool.parseJson[Map[MlsqlOriTable, List[MlsqlIndexer]]](value)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Map()
    }

  }
}
