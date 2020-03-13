package tech.mlsql.plugins.mlsql_watcher.db

import com.fasterxml.jackson.annotation.JsonIgnore
import net.csdn.jpa.QuillDB
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.runtime.AppRuntimeStore
import tech.mlsql.runtime.kvstore.KVIndex

object PluginDB extends Logging {
  val plugin_name = "mlsql_watcher"

  lazy val ctx = {
    val dbWrapper = AppRuntimeStore.store.store.read(classOf[CustomDBWrapper], plugin_name)
    val dbInfo = dbWrapper.db
    logInfo(dbInfo.dbName)
    logInfo(dbInfo.dbConfig)
    QuillDB.createNewCtxByNameFromStr(dbInfo.dbName, dbInfo.dbConfig)
  }
}

case class CustomDBWrapper(db: CustomDB) {
  @JsonIgnore
  @KVIndex
  def id = db.name
}

case class CustomDB(name: String, dbName: String, dbConfig: String)
