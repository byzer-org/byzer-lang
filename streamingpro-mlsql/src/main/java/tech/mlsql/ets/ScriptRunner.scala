package tech.mlsql.ets

import java.util.concurrent.{Callable, Executors}

import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec}
import tech.mlsql.job.{JobManager, MLSQLJobInfo, MLSQLJobType}

/**
  * 2019-06-06 WilliamZhu(allwefantasy@gmail.com)
  */
object ScriptRunner {

  private val executors = Executors.newFixedThreadPool(10)

  def runSubJobAsync(code: String, fetchResult: (DataFrame) => Unit, spark: Option[SparkSession], reuseContext: Boolean, reuseExecListenerEnv: Boolean) = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val finalSpark = spark.getOrElse(context.execListener.sparkSession)
    val jobInfo = JobManager.getJobInfo(context.owner, MLSQLJobType.SCRIPT, context.groupId, code, -1l)
    jobInfo.copy(jobName = jobInfo.jobName + ":" + jobInfo.groupId)
    val future = executors.submit(new Callable[Option[DataFrame]] {
      override def call(): Option[DataFrame] = {
        _run(code, context, jobInfo, finalSpark, fetchResult, reuseContext, reuseExecListenerEnv)
        context.execListener.getLastSelectTable() match {
          case Some(tableName) => Option(finalSpark.table(tableName))
          case None => None
        }

      }
    })
    future
  }

  private def _run(code: String,
                   context: MLSQLExecuteContext,
                   jobInfo: MLSQLJobInfo,
                   spark: SparkSession,
                   fetchResult: (DataFrame) => Unit,
                   reuseContext: Boolean,
                   reuseExecListenerEnv: Boolean) = {

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
      context.execListener.getLastSelectTable() match {
        case Some(tableName) =>
          if (spark.catalog.tableExists(tableName)) {
            val df = spark.table(tableName)
            fetchResult(df)
            Option(df)
          }
          else None
        case None => None
        case None => None
      }
    })
  }

  def rubSubJob(code: String, fetchResult: (DataFrame) => Unit, spark: Option[SparkSession], reuseContext: Boolean, reuseExecListenerEnv: Boolean) = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val finalSpark = spark.getOrElse(context.execListener.sparkSession)

    val jobInfo = JobManager.getJobInfo(context.owner, MLSQLJobType.SCRIPT, context.groupId, code, -1l)
    jobInfo.copy(jobName = jobInfo.jobName + ":" + jobInfo.groupId)
    _run(code, context, jobInfo, finalSpark, fetchResult, reuseContext, reuseExecListenerEnv)
    context.execListener.getLastSelectTable() match {
      case Some(tableName) =>
        if (finalSpark.catalog.tableExists(tableName))
          if (finalSpark.catalog.tableExists(tableName)) {
            val df = finalSpark.table(tableName)
            fetchResult(df)
            Option(df)
          }
          else None
        else None
      case None => None
    }
  }

  def runJob(code: String, jobInfo: MLSQLJobInfo, fetchResult: (DataFrame) => Unit) = {
    val context = ScriptSQLExec.contextGetOrForTest()
    _run(code, context, jobInfo, context.execListener.sparkSession, fetchResult, true, true)

  }

}
