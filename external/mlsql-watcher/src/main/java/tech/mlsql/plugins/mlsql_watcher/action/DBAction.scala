package tech.mlsql.plugins.mlsql_watcher.action

import tech.mlsql.app.CustomController
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.plugins.mlsql_watcher.db.{CustomDB, CustomDBWrapper, PluginDB}
import tech.mlsql.runtime.AppRuntimeStore

/**
 * 10/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class DBAction extends CustomController {
  override def run(params: Map[String, String]): String = {
    val dbName = params("dbName")
    val dbConfig = params("dbConfig")
    AppRuntimeStore.store.store.write(CustomDBWrapper(CustomDB(PluginDB.plugin_name, dbName, dbConfig)))
    JSONTool.toJsonStr(List(Map("msg" -> "success")))
  }
}
