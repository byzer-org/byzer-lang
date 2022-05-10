package org.apache.spark.streaming

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import net.csdn.common.reflect.ReflectHelper
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row}
import serviceframework.dispatcher.{Compositor, StrategyDispatcher}
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec, ScriptSQLExecListener}
import tech.mlsql.common.utils.shell.command.ParamsUtil
import tech.mlsql.ets.ScriptRunner
import tech.mlsql.job.{JobManager, MLSQLJobInfo, MLSQLJobProgress, MLSQLJobType}
import tech.mlsql.runtime.MLSQLPlatformLifecycle

trait SparkOperationUtil {

  def waitJobStarted(groupId: String, timeoutSec: Long = 10) = {
    var count = timeoutSec
    while (JobManager.getJobInfo.filter(f => f._1 == groupId) == 0 && count > 0) {
      Thread.sleep(1000)
      count -= 1
    }
    count > 0
  }

  def waitJobStartedByName(jobName: String, timeoutSec: Long = 10) = {
    var count = timeoutSec
    while (JobManager.getJobInfo.filter(f => f._2.jobName == jobName) == 0 && count > 0) {
      Thread.sleep(1000)
      count -= 1
    }
    count > 0
  }

  def waitWithCondition(shouldWait: () => Boolean, timeoutSec: Long = 10) = {
    var count = timeoutSec
    while (shouldWait() && count > 0) {
      Thread.sleep(1000)
      count -= 1
    }
    count > 0
  }

  def checkJob(runtime: SparkRuntime, groupId: String) = {
    val items = executeCode(runtime,
      """
        |!show jobs;
      """.stripMargin)
    items.filter(r => r.getAs[String]("groupId") == groupId).length == 1
  }

  def getSessionByOwner(runtime: SparkRuntime, owner: String) = {
    runtime.asInstanceOf[SparkRuntime].getSession(owner)
  }

  def getSession(runtime: SparkRuntime) = {
    runtime.asInstanceOf[SparkRuntime].getSession("william")
  }

  def autoGenerateContext(runtime: SparkRuntime, home: String = "/tmp", user: String = "admin", groupId: String = UUID.randomUUID().toString, userDefinedParams: Map[String, String] = Map()) = {
    val exec = new ScriptSQLExecListener(getSessionByOwner(runtime, user), s"${home}/${user}", Map())
    val context = MLSQLExecuteContext(exec, user, s"${home}/${user}", groupId, userDefinedParams)
    context.execListener.addEnv("SKIP_AUTH", "true")
    ScriptSQLExec.setContext(context)
  }

  def createJobInfoFromExistGroupId(code: String) = {
    val context = ScriptSQLExec.context()
    val startTime = System.currentTimeMillis()
    MLSQLJobInfo(context.owner, MLSQLJobType.SCRIPT, UUID.randomUUID().toString, code, context.groupId, new MLSQLJobProgress(0, 0), startTime, -1)
  }

  def executeCodeWithGroupId(runtime: SparkRuntime, groupId: AtomicReference[String], code: String) = {
    autoGenerateContext(runtime)
    groupId.set(ScriptSQLExec.context().groupId)
    val jobInfo = createJobInfoFromExistGroupId(code)
    val holder = new AtomicReference[Array[Row]]()
    ScriptRunner.runJob(code, jobInfo, (df) => {
      holder.set(df.take(100))
    })
    holder.get()
  }

  def executeStreamCode(runtime: SparkRuntime, code: String) = {
    autoGenerateContext(runtime)
    val jobInfo = createJobInfoFromExistGroupId(code)
    val holder = new AtomicReference[Array[Row]]()
    ScriptRunner.runJob(code, jobInfo, (df) => {
      holder.set(df.take(100))
    })
    holder.get()
  }

  def executeCodeWithGroupIdAsync(runtime: SparkRuntime, groupId: AtomicReference[String], code: String): Option[Array[Row]] = {
    new Thread(new Runnable {
      override def run(): Unit = {
        executeCodeWithGroupId(runtime, groupId, code)
      }
    }).start()
    None
  }

  def executeCode(runtime: SparkRuntime, code: String, params: Map[String, String] = Map()) = {
    autoGenerateContext(runtime, userDefinedParams = params)
    val jobInfo = createJobInfoFromExistGroupId(code)
    val holder = new AtomicReference[Array[Row]]()
    ScriptRunner.runJob(code, jobInfo, (df) => {
      holder.set(df.take(100))
    })
    holder.get()

  }

  def executeCode2(home: String, user: String, runtime: SparkRuntime, code: String): Seq[Seq[String]] = {
    autoGenerateContext(runtime, home, user)
    val jobInfo = createJobInfoFromExistGroupId(code)
    val holder = new AtomicReference[Seq[Seq[String]]]()
    ScriptRunner.runJob(code, jobInfo, (df) => {
      val data = df.take(100)

      // For array values, replace Seq and Array with square brackets
      // For cells that are beyond `truncate` characters, replace it with the
      // first `truncate-3` and "..."
      val dataWithSchema = df.schema.fieldNames.toSeq +: data.map { row =>
        row.toSeq.map { cell =>
          val str = cell match {
            case null => "null"
            case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
            case _ => cell.toString
          }
          str
        }: Seq[String]
      }
      holder.set(dataWithSchema)
    })
    holder.get()
  }

  def executeCodeWithoutPhysicalStage(runtime: SparkRuntime, code: String) = {
    autoGenerateContext(runtime)
    val jobInfo = createJobInfoFromExistGroupId(code)
    ScriptSQLExec.context().execListener.addEnv("SKIP_PHYSICAL", "true")
    ScriptRunner.runJob(code, jobInfo, (df) => {})
    ScriptSQLExec.context().execListener.preProcessListener.get
  }

  def executeCodeWithCallback(runtime: SparkRuntime, code: String, f: DataFrame => Unit) = {
    autoGenerateContext(runtime)
    val jobInfo = createJobInfoFromExistGroupId(code)
    ScriptRunner.runJob(code, jobInfo, f)
  }

  def executeCodeAsync(runtime: SparkRuntime, code: String, async: Boolean = false): Option[Array[Row]] = {
    new Thread(new Runnable {
      override def run(): Unit = {
        executeCode(runtime, code)
      }
    }).start()
    None
  }

  def withContext[R](runtime: SparkRuntime)(block: SparkRuntime => R): R = {
    try {
      JobManager.init(runtime.sparkSession)
      block(runtime)
    } finally {
      try {
        JobManager.shutdown
        StrategyDispatcher.clear
        PlatformManager.clear
        runtime.destroyRuntime(false, true)
        val db = new File("./metastore_db")
        FileUtils.deleteQuietly(new File("/tmp/william"))
        if (db.exists()) {
          FileUtils.deleteDirectory(db)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  def withBatchContext[R](runtime: SparkRuntime)(block: SparkRuntime => R): R = {
    try {
      block(runtime)
    } finally {
      try {
        StrategyDispatcher.clear
        PlatformManager.clear
        runtime.destroyRuntime(false, true)
        val db = new File("./metastore_db")
        if (db.exists()) {
          FileUtils.deleteDirectory(db)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  def getCompositorParam(item: Compositor[_]) = {
    ReflectHelper.field(item, "_configParams").
      asInstanceOf[java.util.List[java.util.Map[Any, Any]]]
  }

  def setupBatchContext(batchParams: Array[String], configFilePath: String = null) = {
    var params: ParamsUtil = null
    if (configFilePath != null) {
      val extraParam = Array("-streaming.job.file.path", configFilePath)
      params = new ParamsUtil(batchParams ++ extraParam)
    } else {
      params = new ParamsUtil(batchParams)
    }
    // Start plugins
    Seq("tech.mlsql.runtime.LogFileHook", "tech.mlsql.runtime.PluginHook").foreach{ c =>
      PlatformManager.getOrCreate.registerMLSQLPlatformLifecycle(
        Class.forName(c).
          newInstance().asInstanceOf[MLSQLPlatformLifecycle])
    }

    PlatformManager.getOrCreate.run(params, false)
    val runtime = PlatformManager.getRuntime.asInstanceOf[SparkRuntime]
    runtime
  }

  def appWithBatchContext(batchParams: Array[String], configFilePath: String) = {
    var runtime: SparkRuntime = null
    try {
      val extraParam = Array("-streaming.job.file.path", configFilePath)
      val params = new ParamsUtil(batchParams ++ extraParam)
      PlatformManager.getOrCreate.run(params, false)
      runtime = PlatformManager.getRuntime.asInstanceOf[SparkRuntime]
    } finally {
      try {
        StrategyDispatcher.clear
        PlatformManager.clear
        if (runtime != null) {
          runtime.destroyRuntime(false, true)
        }
        FileUtils.deleteDirectory(new File("./metastore_db"))
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  def tryWithResource[A <: {def close(): Unit}, B](a: A)(f: A => B): B = {
    try f(a)
    finally {
      if (a != null) a.close()
    }
  }
}
