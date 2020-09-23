package tech.mlsql.plugins.healthy

import streaming.dsl.ScriptSQLExec
import tech.mlsql.app.RequestCleaner

/**
 * 23/9/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ShutdownOnSparkContextStoppedCleaner extends RequestCleaner {
  override def run(): Unit = {
    val context = ScriptSQLExec.context()
    val session = context.execListener.sparkSession
    val wow = session.conf.get("spark.mlsql.plugins.healthy.shutdownOnSparkContextStoppedCleaner", "false").toBoolean
    if (wow) {
      if (session.sparkContext.isStopped) {
        System.exit(-1)
      }
    }

  }
}
