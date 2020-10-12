package tech.mlsql.plugins.healthy

import streaming.dsl.ScriptSQLExec
import streaming.log.WowLog
import tech.mlsql.app.RequestCleaner
import tech.mlsql.common.utils.log.Logging

/**
 * 12/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class TempTableCleaner extends RequestCleaner with Logging with WowLog {
  override def run(): Unit = {
    val context = ScriptSQLExec.context()
    val session = context.execListener.sparkSession
    val env = context.execListener.env()
    if (env.getOrElse("__table_name_cache__", "true").toBoolean) {
      return
    }
    context.execListener.declaredTables.foreach { tableName =>
      if (env.getOrElse("__debug__", "false").toBoolean) {
        logInfo(format(s"TempTableCleaner => clean ${tableName}"))
      }
      session.catalog.dropTempView(tableName)
    }
  }
}
