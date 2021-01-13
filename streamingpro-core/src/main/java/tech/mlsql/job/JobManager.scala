package tech.mlsql.job

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.mlsql.session.{SessionIdentifier, SparkSessionCacheManager}
import streaming.dsl.ScriptSQLExec
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.job.JobListener.{JobFinishedEvent, JobStartedEvent}
import tech.mlsql.job.listeners.{CleanCacheListener, EngineMDCLogListener}
import tech.mlsql.runtime.plugins.request_cleaner.RequestCleanerManager

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * 2019-04-07 WilliamZhu(allwefantasy@gmail.com)
 */
object JobManager extends Logging {
  private[this] var _jobManager: JobManager = _
  private[this] val _executor = Executors.newFixedThreadPool(100)
  private[this] val _jobListeners = ArrayBuffer[JobListener]()

  def addJobListener(listener: JobListener) = {
    _jobListeners += listener
  }

  def removeJobListener(listener: JobListener) = {
    _jobListeners -= listener
  }

  def shutdown = {
    logInfo(s"JobManager is shutdown....")
    _executor.shutdownNow()
    _jobManager.shutdown
    _jobManager = null
    _jobListeners.clear()
  }

  def init(spark: SparkSession, initialDelay: Long = 30, checkTimeInterval: Long = 5) = {
    synchronized {
      if (_jobManager == null) {
        logInfo(s"JobManager started with initialDelay=${initialDelay} checkTimeInterval=${checkTimeInterval}")
        _jobManager = new JobManager(spark, initialDelay, checkTimeInterval)
        _jobListeners += new CleanCacheListener
        _jobListeners += new EngineMDCLogListener
        _jobManager.run
      }
    }
  }

  def initForTest(spark: SparkSession, initialDelay: Long = 30, checkTimeInterval: Long = 5) = {
    if (_jobManager == null) {
      logInfo(s"JobManager started with initialDelay=${initialDelay} checkTimeInterval=${checkTimeInterval}")
      _jobManager = new JobManager(spark, initialDelay, checkTimeInterval)
      _jobListeners += new CleanCacheListener
    }
  }

  def run(session: SparkSession, job: MLSQLJobInfo, f: () => Unit): Unit = {

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.addJobProgressListener(new DefaultMLSQLJobProgressListener())
    try {
      _jobListeners.foreach { f => f.onJobStarted(new JobStartedEvent(job.groupId)) }
      if (_jobManager == null) {
        f()
      } else {
        session.sparkContext.setJobGroup(job.groupId, job.jobName, true)
        _jobManager.groupIdToMLSQLJobInfo.put(job.groupId, job)
        f()
      }

    } finally {
      handleJobDone(job.groupId)
      session.sparkContext.clearJobGroup()
      _jobListeners.foreach { f => f.onJobFinished(new JobFinishedEvent(job.groupId)) }
    }
  }

  def asyncRun(session: SparkSession, job: MLSQLJobInfo, f: () => Unit) = {
    // TODO: (fchen) 改成callback
    val context = ScriptSQLExec.contextGetOrForTest()
    _executor.execute(new Runnable {
      override def run(): Unit = {
        ScriptSQLExec.setContext(context)
        try {
          JobManager.run(session, job, f)
          context.execListener.addEnv("__MarkAsyncRunFinish__","true")
        } catch {
          case e: Exception =>
            logInfo("Async Job Exception", e)
        } finally {
          RequestCleanerManager.call()
          context.execListener.env.remove("__MarkAsyncRunFinish__")
          ScriptSQLExec.unset
          SparkSession.clearActiveSession()
        }

      }
    })
  }

  def getJobInfo(owner: String,
                 jobType: String,
                 jobName: String,
                 jobContent: String,
                 timeout: Long): MLSQLJobInfo = {
    val startTime = System.currentTimeMillis()
    val groupId = _jobManager.nextGroupId
    MLSQLJobInfo(owner, jobType, jobName, jobContent, groupId, new MLSQLJobProgress(0, 0), startTime, timeout)
  }

  def getJobInfo: Map[String, MLSQLJobInfo] =
    _jobManager.groupIdToMLSQLJobInfo.asScala.toMap

  def addJobManually(job: MLSQLJobInfo) = {
    _jobManager.groupIdToMLSQLJobInfo.put(job.groupId, job)
  }

  def removeJobManually(groupId: String) = {
    handleJobDone(groupId)
  }

  def killJob(session: SparkSession, groupId: String): Unit = {
    _jobManager.cancelJobGroup(session, groupId)
  }

  private def handleJobDone(groupId: String): Unit = {
    _jobManager.groupIdToMLSQLJobInfo.remove(groupId)

  }
}

class JobManager(_spark: SparkSession, initialDelay: Long, checkTimeInterval: Long) extends Logging with WowLog {
  val groupIdToMLSQLJobInfo = new ConcurrentHashMap[String, MLSQLJobInfo]()


  def nextGroupId = UUID.randomUUID().toString

  val executor = Executors.newSingleThreadScheduledExecutor()

  def run = {
    executor.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        groupIdToMLSQLJobInfo.foreach { f =>
          try {
            val elapseTime = System.currentTimeMillis() - f._2.startTime
            if (f._2.timeout > 0 && elapseTime >= f._2.timeout) {

              // At rest controller, we will clone the session,and this clone session is not
              // saved in  SparkSessionCacheManager. But this do no harm to this scheduler,
              // since cancel job depends `groupId` and sparkContext. The exception is stream job (which is connected with spark session),
              // however, the stream job will not use `clone spark session`
              val tempSession = SparkSessionCacheManager.getSessionManagerOption match {
                case Some(sessionManager) =>
                  sessionManager.getSessionOption(SessionIdentifier(f._2.owner))
                case None => None
              }
              val session = tempSession.map(f => f.sparkSession).getOrElse(_spark)
              cancelJobGroup(session, f._1, true)
            }
          } catch {
            case e: Exception => logError(format(s"Kill job ${f._1} fails"), e)
          }
        }
      }
    }, initialDelay, checkTimeInterval, TimeUnit.SECONDS)
  }

  def cancelJobGroup(spark: SparkSession, groupId: String, ignoreStreamJob: Boolean = false): Unit = {
    logInfo(format("JobManager Timer cancel job group " + groupId))
    val job = groupIdToMLSQLJobInfo.get(groupId)

    // when we try to kill stream job, we do not need to remove it from  groupIdToMLSQLJobInfo here.
    // This is because once we kill the stream job successfully,
    // then the MLSQLExternalStreamListener.onQueryTerminated will be invoked and remove it from  groupIdToMLSQLJobInfo.
    // If we now remove it from JobManager.groupIdToMLSQLJobInfo, and finally the stream job is not killed, then
    // the state of JobManager.groupIdToMLSQLJobInfo is not consistent with spark streams.
    def killStreamJob = {
      spark.streams.active.filter(f => f.id.toString == job.groupId).map(f => f.runId.toString).headOption match {
        case Some(_) =>
          logInfo(format(s"Try to kill stream job: ${job.groupId}, name:${job.jobName} "))
          spark.streams.get(job.groupId).stop()
        case None => logWarning(format(s"the stream job: ${job.groupId}, name:${job.jobName} is not in spark.streams."))
      }
    }

    def killBatchJob = {
      spark.sparkContext.cancelJobGroup(groupId)
      groupIdToMLSQLJobInfo.remove(groupId)
    }

    if (job != null && !ignoreStreamJob && job.jobType == MLSQLJobType.STREAM) {
      killStreamJob
    }

    if (job.jobType != MLSQLJobType.STREAM) {
      killBatchJob
    }
  }

  def shutdown = {
    executor.shutdownNow()
  }
}

case object MLSQLJobType {
  val SCRIPT = "script"
  val SQL = "sql"
  val STREAM = "stream"
}

trait MLSQLJobProgressListener {
  def before(name: String, sql: String): Unit

  def after(name: String, sql: String): Unit
}

class DefaultMLSQLJobProgressListener extends MLSQLJobProgressListener with Logging with WowLog {

  val actionSet = Set("save", "insert", "train", "run", "predict")
  var counter = 0

  override def before(name: String, sql: String): Unit = {
    counter += 1
    val context = ScriptSQLExec.contextGetOrForTest()
    val job = JobManager.getJobInfo.filter(f => f._1 == context.groupId).head._2
    // only save/insert will trigger execution

    def getHead(str: String) = {
      str.trim.toLowerCase().split("\\s+").head
    }

    val statements = context.execListener.preProcessListener.get.statements

    val actions = statements.filter { statement =>
      actionSet.contains(getHead(statement))
    }

    var finalSize = actions.size
    if (!actionSet.contains(getHead(statements.last))) {
      finalSize += 1
    }
    var shouldLog = false

    if (actionSet.contains(name)) {
      job.progress.increment
      job.progress.script = sql
      shouldLog = true
    }
    job.progress.totalJob = finalSize

    if (counter == statements.size && !actionSet.contains(name)) {
      job.progress.currentJobIndex = job.progress.totalJob
      job.progress.script = sql
      shouldLog = true
    }
    if (shouldLog && !job.progress.script.startsWith("load _mlsql_.")) {
      logInfo(format(s"Total jobs: ${job.progress.totalJob} current job:${job.progress.currentJobIndex} job script:${job.progress.script} "))
    }


  }

  override def after(name: String, sql: String): Unit = {

  }
}


case class MLSQLJobInfo(
                         owner: String,
                         jobType: String,
                         jobName: String,
                         jobContent: String,
                         groupId: String,
                         progress: MLSQLJobProgress,
                         startTime: Long,
                         timeout: Long
                       )

case class MLSQLJobProgress(var totalJob: Long = 0, var currentJobIndex: Long = 0, var script: String = "") {
  def increment = currentJobIndex += 1

  def setTotal(total: Long) = {
    totalJob = total
  }
}

