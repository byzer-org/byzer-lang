package tech.mlsql.plugins.cleaner

import org.apache.spark.sql.CleanerUtils
import streaming.dsl.ScriptSQLExec
import streaming.log.WowLog
import tech.mlsql.app.RequestCleaner
import tech.mlsql.common.utils.log.Logging

/**
 * 13/1/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class SessionCleaner extends RequestCleaner with Logging with WowLog {
  override def run(): Unit = {
    val context = ScriptSQLExec.context()
    val session = context.execListener.sparkSession
    val bus = CleanerUtils.listenerBus(session.sparkContext)
    val removeListener = CleanerUtils.filterExecutionListenerBusWithSession(bus, session)
    logInfo(s"clean ${session} ${removeListener.toList}")
    removeListener.foreach { removeListener =>
      session.sparkContext.removeSparkListener(removeListener)
    }
  }
}
