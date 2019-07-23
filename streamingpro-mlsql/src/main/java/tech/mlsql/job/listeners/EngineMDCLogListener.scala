package tech.mlsql.job.listeners

import org.slf4j.MDC
import streaming.dsl.ScriptSQLExec
import tech.mlsql.MLSQLEnvKey
import tech.mlsql.job.JobListener

/**
  * 2019-06-01 WilliamZhu(allwefantasy@gmail.com)
  */
class EngineMDCLogListener extends JobListener {
  override def onJobStarted(event: JobListener.JobStartedEvent): Unit = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val items = context.userDefinedParam
    if (items.getOrElse(MLSQLEnvKey.REQUEST_CONTEXT_ENABLE_SPARK_LOG, "false").toBoolean) {
      MDC.put("owner", s"[owner] [${context.owner}]")
    }

  }

  override def onJobFinished(event: JobListener.JobFinishedEvent): Unit = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val items = context.userDefinedParam
    if (items.getOrElse(MLSQLEnvKey.REQUEST_CONTEXT_ENABLE_SPARK_LOG, "false").toBoolean) {
      MDC.remove("owner")
    }
  }
}
