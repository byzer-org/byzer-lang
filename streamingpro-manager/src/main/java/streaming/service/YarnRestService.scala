package streaming.service

import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import com.google.inject.Singleton
import net.csdn.common.logging.{CSLogger, Loggers}
import net.csdn.common.settings.{ImmutableSettings, Settings}
import net.csdn.modules.threadpool.DefaultThreadPoolService
import net.csdn.modules.transport.{DefaultHttpTransportService, HttpTransportService}
import net.csdn.modules.transport.proxy.{AggregateRestClient, FirstMeetProxyStrategy}
import org.apache.log4j.Logger
import streaming.db.{DB, ManagerConfiguration, TSparkApplication, TSparkApplicationLog}
import streaming.remote.YarnApplicationState.YarnApplicationState
import streaming.remote.{YarnApplication, YarnController}
import streaming.shell.{AsyncShellCommand, Md5, ShellCommand}

import scala.collection.JavaConversions._
import streaming.remote.YarnControllerE._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 12/7/2017.
  */
class Scheduler {
  Scheduler.start
}

object Scheduler {


  DB

  val logger = Loggers.getLogger(classOf[Scheduler])
  val sparkSubmitTaskMap = new ConcurrentHashMap[Task, Long]()
  val resubmitMap = new ConcurrentHashMap[Long, Long]()

  val buffer = new ArrayBuffer[Any]()


  //scheduler
  val livenessCheckScheduler = Executors.newSingleThreadScheduledExecutor()
  val sparkSchedulerCleanerScheduler = Executors.newSingleThreadScheduledExecutor()
  val sparkLogCheckerScheduler = Executors.newSingleThreadScheduledExecutor()

  def shutdown = {
    try {
      livenessCheckScheduler.shutdownNow()
      sparkLogCheckerScheduler.shutdownNow()
      sparkSchedulerCleanerScheduler.shutdownNow()
    } catch {
      case e: Exception =>
        logger.info("shutdown livenessCheckScheduler,sparkLogCheckerScheduler,sparkSchedulerCleanerScheduler error", e)
    }

  }

  def start = {
    buffer += livenessCheckScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        try {
          val apps = TSparkApplication.list
          apps.filter(app => shouldStart(app, app.url)).foreach { targetApp =>
            logger.info(s"Find ${targetApp.applicationId} fail; Resubmit it ")
            submitApp(targetApp)
          }
        } catch {
          case e: Exception =>
            logger.info("livenessCheckScheduler fail")
        }
      }
    }
      , 1, ManagerConfiguration.liveness_check_interval, TimeUnit.SECONDS)

    buffer += sparkLogCheckerScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        try {
          import scala.collection.JavaConversions._

          sparkSubmitTaskMap.foreach { taskAndTime =>
            checkSubmitAppStateTask(taskAndTime._1.appId, taskAndTime._1)
          }
        } catch {
          case e: Exception =>
            logger.info("sparkLogCheckerScheduler fail")
        }

      }
    }, 1, ManagerConfiguration.submit_progress_check_interval, TimeUnit.SECONDS)

    buffer += sparkSchedulerCleanerScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        import scala.collection.JavaConversions._
        try {
          sparkSubmitTaskMap.filter { taskAndTime =>
            val elipseTime = System.currentTimeMillis() - taskAndTime._2
            elipseTime > ManagerConfiguration.submit_progress_check_expire_duration * 1000 //ten minutes expire
          }.foreach { task =>
            sparkSubmitTaskMap.remove(task)
            logger.error(s"Cleaner clean  task monitoring submit " +
              s" log ${task._1.taskId} in ${task._1.host};" +
              s" Cause not removed in ten minutes")
          }


          resubmitMap.filter { idAndTime =>
            val elipseTime = System.currentTimeMillis() - idAndTime._2
            elipseTime > ManagerConfiguration.submit_progress_check_expire_duration * 1000 / 2 //five minutes expire
          }.foreach { submit =>
            resubmitMap.remove(submit._1)
            logger.error(s"Cleaner clean  reSubmit App where id= ${submit._1}" +
              s" Cause not removed in ten minutes")
          }
        } catch {
          case e: Exception =>
            logger.info("sparkSchedulerCleanerScheduler fail")
        }


      }
    }, 1, ManagerConfiguration.clean_check_interval, TimeUnit.SECONDS)
  }


  def submitApp(app: TSparkApplication) = {
    val prefix = "export SPARK_HOME=/opt/spark-2.1.1;export HADOOP_CONF_DIR=/etc/hadoop/conf;cd $SPARK_HOME;"
    //val prefix = "source /etc/profile ;cd $SPARK_HOME ;"
    var shellCommand = s"./bin/${app.source}"
    if (shellCommand.contains("--master yarn") && shellCommand.contains("--deploy-mode client")) {
      val fakeTaskId = "_" + Md5.MD5(shellCommand) + "_" + System.currentTimeMillis()
      ShellCommand.exec(s"mkdir -p /tmp/mammuthus/${fakeTaskId}")
      shellCommand = s"""$prefix nohup  ${shellCommand} > /tmp/mammuthus/${fakeTaskId}/stdout 2>&1 &"""
      logger.info(shellCommand)
      logger.info(ShellCommand.exec(shellCommand))
      logger.info(s"spark yarn client mode : Put submit task ${Task(fakeTaskId, "", app.id)} in sparkSubmitTaskMap")
      sparkSubmitTaskMap.put(Task(fakeTaskId, "", app.id), System.currentTimeMillis())
      (fakeTaskId, "")
    } else {
      val taskId = AsyncShellCommand.start(prefix + shellCommand, System.currentTimeMillis() + "", false)
      logger.info(s"spark yanr cluster/local mode: Put submit task ${Task(taskId, "", app.id)} in sparkSubmitTaskMap")
      sparkSubmitTaskMap.put(Task(taskId, "", app.id), System.currentTimeMillis())
      (taskId, "")
    }

  }

  def checkSubmitAppStateTask(appId: Long, task: Task) = {
    val host = task.host
    val taskId = task.taskId

    val shellProcessResult = if (taskId.startsWith("_")) {
      ShellCommand.exec(s"cat /tmp/mammuthus/${taskId}/stdout")
    } else {
      val res = AsyncShellCommand.progress(taskId, 0)
      if (res != null) {
        res._2._2
      } else null
    }
    if (shellProcessResult != null) {
      //Application report for application_1457496292231_0022 (state: ACCEPTED)
      val app = TSparkApplication.find(appId).get
      val accepLines = shellProcessResult.split("\n").
        filter(line => line.contains("state: ACCEPTED"))
      if (accepLines.size > 0) {
        val applicationId = accepLines.head.split("\\s").filter(block => block.startsWith("application_")).head
        app.parentApplicationId = app.applicationId
        app.applicationId = applicationId
        logger.info("New application submitted success. ApplicationId =" + applicationId)
        TSparkApplication.reSave(app)
        logger.info(s"Remove submit check task ${task} from sparkSubmitTaskMap")
        sparkSubmitTaskMap.remove(task)
        resubmitMap.remove(app.id)
      } else {
        //logger.info(s"check application submit state from taskId:${accepLines.mkString("\n")}")
      }
    }
  }


  def shouldStart(app: TSparkApplication, yarnUrl: String): Boolean = {
    if (!TSparkApplication.shouldWatch(app)) return false

    if (resubmitMap.containsKey(app.id) && System.currentTimeMillis() - resubmitMap.get(app.id) < 1000 * ManagerConfiguration.resubmit_try_interval) {
      return false
    }
    if (resubmitMap.containsKey(app.id) && System.currentTimeMillis() - resubmitMap.get(app.id) > 1000 * ManagerConfiguration.resubmit_try_interval) {
      logger.error(s"app.id=${app.id} try to start but failed in one minute")
      resubmitMap.remove(app.id)
    }
    val appAlreadySubmit = YarnRestService.query(yarnUrl, (client, rm) => {
      val tempAppList = client.apps(
        List(
          YarnApplicationState.RUNNING,
          YarnApplicationState.ACCEPTED,
          YarnApplicationState.SUBMITTED,
          YarnApplicationState.NEW,
          YarnApplicationState.NEW_SAVING
        ).mkString(",")
      ).app()
      val name = app.source.split("--class").last.trim.split("\\s+").head

      tempAppList.map(f => f.name == name).size > 0

    })

    if (appAlreadySubmit) return false

    val targetApp = YarnRestService.query(yarnUrl, (client, rm) => {
      val tempAppList = client.app(app.applicationId)
      if (tempAppList.size() == 0) {
        throw new RuntimeException(s"yarn.rm.address=${rm} reqeust fail.Please check RM")
      }
      val tempApp = tempAppList.get(0)
      if (tempApp.getStatus != 200) throw new RuntimeException(s"yarn.rm.address=${rm} reqeust fail.Please check RM")
      tempAppList.app()(0)
    })
    val result = (targetApp != null && (targetApp.state == YarnApplicationState.FAILED.toString ||
      targetApp.state == YarnApplicationState.KILLED.toString ||
      targetApp.state == YarnApplicationState.FINISHED.toString))

    if (result) {
      resubmitMap.put(app.id, System.currentTimeMillis())
      logger.error(s"${targetApp.id} ${app.url} ${targetApp.diagnostics} ${app.startTime}")
      TSparkApplication.saveLog(new TSparkApplicationLog(0,
        targetApp.id,
        app.url,
        app.source, "",
        targetApp.diagnostics,
        app.startTime,
        System.currentTimeMillis()))
    }
    result
  }
}


object YarnRestService {

  private val firstMeetProxyStrategy = new FirstMeetProxyStrategy()


  def yarnRestClient(hostAndPort: String): YarnController = AggregateRestClient.buildIfPresent[YarnController](hostAndPort, firstMeetProxyStrategy, RestClient.transportService)

  def query[T](resourceManagersStr: String, f: (YarnController, String) => T) = {
    val resourceManagers = resourceManagersStr.split(",").toList
    try {
      f(yarnRestClient(resourceManagers(0)), resourceManagers(0))
    }
    catch {
      case e: Exception =>
        if (resourceManagers.size > 1)
          f(yarnRestClient(resourceManagers(1)), resourceManagers(1))
        else null.asInstanceOf[T]
    }
  }

  def yarnApps(resourceManagers: String, states: List[YarnApplicationState]) = {
    val statesParam = states.map(state => state.toString).mkString(",")
    query[List[YarnApplication]](resourceManagers, (yarnController, _) =>
      yarnController.apps(statesParam).apps()
    )
  }

  def findApp(resourceManagers: String, appId: String) = {
    query[List[YarnApplication]](resourceManagers, (yarnController, _) =>
      yarnController.app(appId).app()
    )
  }

  def isRunning(resourceManagers: String, appId: String): Boolean = {
    val apps = findApp(resourceManagers, appId)
    isRunning(apps)
  }

  def isRunning(apps: List[YarnApplication]): Boolean = {
    apps.size > 0 && apps(0).state == YarnApplicationState.RUNNING.toString
  }

}

case class Task(taskId: String, host: String, appId: Long)

object YarnApplicationState extends Enumeration {
  type YarnApplicationState = Value
  val NEW = Value("NEW")
  val NEW_SAVING = Value("NEW_SAVING")
  val SUBMITTED = Value("SUBMITTED")
  val ACCEPTED = Value("ACCEPTED")
  val RUNNING = Value("RUNNING")
  val FINISHED = Value("FINISHED")
  val FAILED = Value("FAILED")
  val KILLED = Value("KILLED")
}

object RestClient {
  private final val settings: Settings = ImmutableSettings.settingsBuilder().loadFromClasspath("application.yml").build()
  final val transportService: HttpTransportService = new DefaultHttpTransportService(new DefaultThreadPoolService(settings), settings)
}

class RestClient

