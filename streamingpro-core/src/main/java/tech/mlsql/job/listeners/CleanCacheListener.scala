package tech.mlsql.job.listeners

import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.SQLCacheExt
import tech.mlsql.job.JobListener

/**
  * 2019-04-18 WilliamZhu(allwefantasy@gmail.com)
  */
class CleanCacheListener extends JobListener {
  override def onJobStarted(event: JobListener.JobStartedEvent): Unit = {

  }

  override def onJobFinished(event: JobListener.JobFinishedEvent): Unit = {
    val context = ScriptSQLExec.contextGetOrForTest()

    def isStream = {
      context.execListener.env().contains("stream")
    }

    if (!isStream) {
      SQLCacheExt.cleanCache(context.execListener.sparkSession, event.groupId)
    }

  }
}
