package tech.mlsql.ets

import java.util.concurrent.{Callable, Executors}

import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec}
import tech.mlsql.job.{JobManager, MLSQLJobType}

/**
  * 2019-06-06 WilliamZhu(allwefantasy@gmail.com)
  */
object ScriptRunner {

  private val executors = Executors.newFixedThreadPool(10)

  def runAsync(code: String, spark: Option[SparkSession], reuseContext: Boolean, reuseExecListenerEnv: Boolean) = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val finalSpark = spark.getOrElse(context.execListener.sparkSession)
    val future = executors.submit(new Callable[Option[DataFrame]] {
      override def call(): Option[DataFrame] = {
        _run(code, context, finalSpark, reuseContext, reuseExecListenerEnv)
        context.execListener.getLastSelectTable() match {
          case Some(tableName) => Option(finalSpark.table(tableName))
          case None => None
        }

      }
    })
    future
  }

  private def _run(code: String, context: MLSQLExecuteContext, spark: SparkSession, reuseContext: Boolean, reuseExecListenerEnv: Boolean) = {
    val jobInfo = JobManager.getJobInfo(context.owner, MLSQLJobType.SCRIPT, context.groupId, code, -1l)
    jobInfo.copy(jobName = jobInfo.jobName + ":" + jobInfo.groupId)

    JobManager.run(spark, jobInfo, () => {

      val newContext = if (!reuseContext) {
        val ssel = context.execListener.clone(spark)
        val newContext = new MLSQLExecuteContext(ssel, context.owner, context.home, jobInfo.groupId, context.userDefinedParam)
        ScriptSQLExec.setContext(newContext)
        if (!reuseExecListenerEnv) {
          newContext.execListener.env().clear()
        }
        newContext
      } else context
      val skipAuth = newContext.execListener.env().getOrElse("SKIP_AUTH", "false").toBoolean
      ScriptSQLExec.parse(code, newContext.execListener, false, skipAuth, false, false)
    })
  }

  def run(code: String, spark: Option[SparkSession], reuseContext: Boolean, reuseExecListenerEnv: Boolean) = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val finalSpark = spark.getOrElse(context.execListener.sparkSession)
    _run(code, context, finalSpark, reuseContext, reuseExecListenerEnv)
    context.execListener.getLastSelectTable() match {
      case Some(tableName) => Option(finalSpark.table(tableName))
      case None => None
    }
  }
}
