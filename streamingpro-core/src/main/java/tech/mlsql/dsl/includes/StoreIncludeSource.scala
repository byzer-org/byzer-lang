package tech.mlsql.dsl.includes

import java.nio.charset.Charset

import org.apache.http.client.fluent.{Form, Request}
import org.apache.spark.sql.SparkSession
import streaming.dsl.IncludeSource
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.runtime.PluginStoreItem

/**
 * 2019-05-12 WilliamZhu(allwefantasy@gmail.com)
 */
class StoreIncludeSource extends IncludeSource with Logging {

  val PLUGIN_STORE_URL = "http://store.mlsql.tech/run"

  def getPluginInfo(name: String) = {
    val pluginListResponse = Request.Post(PLUGIN_STORE_URL).connectTimeout(60 * 1000)
      .socketTimeout(60 * 60 * 1000).bodyForm(Form.form().
      add("action", "getPlugin").
      add("pluginName", name).
      add("pluginType", "MLSQL_SCRIPT").
      build(),
      Charset.forName("utf-8")).execute().returnContent().asString(Charset.forName("utf-8"))
    JSONTool.parseJson[List[PluginStoreItem]](pluginListResponse)
  }

  def getLatestPluginInfo(name: String) = {
    val plugins = getPluginInfo(name)
    plugins.sortBy(_.version).last
  }

  def getPluginNameAndVersion(name: String): (String, String) = {
    if (name.contains(":")) {
      name.split(":") match {
        case Array(name, version) => (name, version)
      }
    } else {
      (name, getLatestPluginInfo(name).version)
    }
  }

  override def fetchSource(sparkSession: SparkSession, path: String, options: Map[String, String]): String = {
    val (name, version) = getPluginNameAndVersion(path)
    val pluginInfoOpt = getPluginInfo(name).filter(item => item.version == version).headOption
    require(pluginInfoOpt.isDefined, s"Plugin ${name}:${version} is not exists")
    val params = JSONTool.parseJson[Map[String, String]](pluginInfoOpt.get.extraParams)
    params("content")
  }

  override def skipPathPrefix: Boolean = true
}
