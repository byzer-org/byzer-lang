package tech.mlsql.plugins.mlsql_watcher

import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.plugins.mlsql_watcher.action.DBAction
import tech.mlsql.runtime.AppRuntimeStore
import tech.mlsql.version.VersionCompatibility

/**
 * 10/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLWatcher extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    AppRuntimeStore.store.registerController("/watcher/db", classOf[DBAction].getName)
    ETRegister.register("MLSQLWatcherCommand", "tech.mlsql.plugins.mlsql_watcher.MLSQLWatcherCommand")
    CommandCollection.refreshCommandMapping(Map("watcher" -> "MLSQLWatcherCommand"))
    SnapshotTimer.start()
  }

  override def supportedVersions: Seq[String] = Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
}

