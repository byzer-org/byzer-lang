package tech.mlsql.plugins.cleaner

import org.apache.spark.sql.CleanerUtils
import streaming.dsl.ScriptSQLExec
import streaming.log.WowLog
import tech.mlsql.app.RequestCleaner
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool

/**
 * 13/1/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class SessionCleaner extends RequestCleaner with Logging with WowLog {
  override def run(): Unit = {
    val context = ScriptSQLExec.context()
    val ifAsync = JSONTool.parseJson[Map[String, String]](context.userDefinedParam("__PARAMS__")).getOrElse("async", "false").toBoolean

    def cleanSessionListener = {
      val session = context.execListener.sparkSession
      val bus = CleanerUtils.listenerBus(session.sparkContext)
      val removeListener = CleanerUtils.filterExecutionListenerBusWithSession(bus, session)
      logInfo(s"clean ${session} ${removeListener.toList}")
      synchronized {
        removeListener.foreach { removeListener =>
          session.sparkContext.removeSparkListener(removeListener)
        }
      }
    }
    //异步请求，并且不是在任务线程中，则返回不清理
    if (ifAsync && !context.execListener.env().getOrElse("__MarkAsyncRunFinish__", "false").toBoolean) {
      return
    }
    cleanSessionListener

  }
}
