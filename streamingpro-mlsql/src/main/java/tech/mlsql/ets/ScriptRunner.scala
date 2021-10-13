package tech.mlsql.ets

import java.util
import java.util.concurrent.{Callable, Executors}

import net.csdn.common.exception.RenderFinish
import net.csdn.modules.http.DefaultRestRequest
import net.csdn.modules.mock.MockRestResponse
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec}
import streaming.rest.RestController
import tech.mlsql.common.utils.lang.sc.{ScalaMethodMacros => LIT}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.job.{JobManager, MLSQLJobInfo, MLSQLJobType}

/**
 * 2019-06-06 WilliamZhu(allwefantasy@gmail.com)
 */
object ScriptRunner extends Logging {

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
        List("SKIP_AUTH", "HOME", "OWNER").foreach { item =>
          newContext.execListener.env().put(item, context.execListener.env().get(item).get)
        }
        newContext
      } else context
      val skipAuth = newContext.execListener.env().getOrElse("SKIP_AUTH", "false").toBoolean
      val skipPhysical = newContext.execListener.env().getOrElse("SKIP_PHYSICAL", "false").toBoolean
      ScriptSQLExec.parse(code, newContext.execListener, false, skipAuth, skipPhysical, false)
      context.execListener.getLastSelectTable() match {
        case Some(tableName) =>
          if (spark.catalog.tableExists(tableName)) {
            val df = spark.table(tableName)
            fetchResult(df)
            Option(df)
          }
          else None
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

  /**
   * Example:
   *
   * val timeout = JobManager.getJobInfo.get(context.groupId).get.timeout
   * val code =
   * """
   * |
   * """.stripMargin
   * val jobInfo = JobManager.getJobInfo(context.owner, MLSQLJobType.SCRIPT, "", code, timeout)
   *         ScriptRunner.runJob(code, jobInfo, (df) => {
   *
   * })
   *
   */
  def runJob(code: String, jobInfo: MLSQLJobInfo, fetchResult: (DataFrame) => Unit) = {
    val context = ScriptSQLExec.contextGetOrForTest()
    _run(code, context, jobInfo, context.execListener.sparkSession, fetchResult, true, true)

  }

  def jRunLikeAction(params: java.util.Map[String, String], skipGetResult: Boolean) = {
    try {
      params.put("silence", skipGetResult.toString)
      val restRequest = new DefaultRestRequest("POST", params)
      val restReponse = new MockRestResponse()
      val controller = new RestController()
      net.csdn.modules.http.RestController.enhanceApplicationController(controller, restRequest, restReponse)
      try {
        controller.script
      } catch {
        case _: RenderFinish =>
      }
      if (skipGetResult) {
        val context = ScriptSQLExec.context().execListener
        val value = context.getLastSelectTable() match {
          case Some(tableName) =>
            Some(context.sparkSession.table(tableName))
          case None =>
            None
        }
        Right(value)
      } else {
        val jsonStr = restReponse.content()
        Left(jsonStr)
      }

    } catch {
      case e: Exception =>
        logInfo("MLSQL execution fails", e)
        if (skipGetResult) Left("{}") else Right(None)
    }
  }

  def runLikeAction(params: Map[String, String], skipGetResult: Boolean): Either[String, Option[DataFrame]] = {
    val newParams = new util.HashMap[String, String]()
    params.foreach(kv => newParams.put(kv._1, kv._2))
    jRunLikeAction(newParams, skipGetResult)
  }

  def runLikeAction(actionParams: ActionParams, skipGetResult: Boolean): Either[String, Option[DataFrame]] = {
    val newParams = new util.HashMap[String, String]()
    actionParams.options.foreach(kv => newParams.put(kv._1, kv._2))

    newParams.put(LIT.str(actionParams.sql), actionParams.sql)
    newParams.put(LIT.str(actionParams.owner), actionParams.owner)
    newParams.put(LIT.str(actionParams.jobName), actionParams.jobName)
    newParams.put(LIT.str(actionParams.includeSchema), if (actionParams.includeSchema) "true" else "false")
    newParams.put(LIT.str(actionParams.fetchType), actionParams.fetchType)

    jRunLikeAction(newParams, skipGetResult)
  }
}
