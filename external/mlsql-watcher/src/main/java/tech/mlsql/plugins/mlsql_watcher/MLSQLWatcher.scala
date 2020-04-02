package tech.mlsql.plugins.mlsql_watcher

import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.plugins.mlsql_watcher.action.DBAction
import tech.mlsql.plugins.mlsql_watcher.db.{CustomDB, CustomDBWrapper, PluginDB}
import tech.mlsql.runtime.AppRuntimeStore
import tech.mlsql.store.{DBStore, DictType}
import tech.mlsql.version.VersionCompatibility

/**
 * 10/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLWatcher extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    AppRuntimeStore.store.registerController("/watcher/db", classOf[DBAction].getName)
    ETRegister.register("MLSQLWatcherCommand", "tech.mlsql.plugins.mlsql_watcher.MLSQLWatcherCommand")
    CommandCollection.refreshCommandMapping(Map("watcher" -> "MLSQLWatcherCommand"))
    val root = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].sparkSession
    DBStore.store.readConfig(root, PluginDB.plugin_name, PluginDB.plugin_name, DictType.APP_TO_DB).headOption match {
      case Some(config) =>
        DBStore.store.readConfig(root, PluginDB.plugin_name, config.value, DictType.DB).headOption match {
          case Some(db) =>
            AppRuntimeStore.store.store.write(CustomDBWrapper(CustomDB(PluginDB.plugin_name, config.value, db.value)))
          case None =>
        }
      case None =>
    }
    SnapshotTimer.start()
  }

  override def supportedVersions: Seq[String] = Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
}

