package streaming.core.stream

import java.util.concurrent.Executors

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import streaming.crawler.HttpClientCrawler
import streaming.dsl.ScriptSQLExec
import streaming.log.{Logging, WowLog}
import tech.mlsql.job.{JobManager, MLSQLJobInfo, MLSQLJobType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * 2019-01-21 WilliamZhu(allwefantasy@gmail.com)
  */
object MLSQLStreamManager extends Logging with WowLog {

  val executors = Executors.newFixedThreadPool(30)
  private val store = new java.util.concurrent.ConcurrentHashMap[String, MLSQLJobInfo]()
  private val _listenerStore = new java.util.concurrent.ConcurrentHashMap[String, ArrayBuffer[MLSQLExternalStreamListener]]()

    def listeners() = {
    _listenerStore
  }

  def runEvent(eventName: MLSQLStreamEventName.eventName, streamName: String, callback: (MLSQLExternalStreamListener) => Unit) = {
    _listenerStore.asScala.foreach { case (user, items) =>
      items.filter(p => p.item.eventName == eventName && p.item.streamName == streamName).foreach { p =>
        callback(p)
      }
    }
  }

  def addListener(name: String, item: MLSQLStreamListenerItem) = {
    synchronized {
      val buffer = _listenerStore.getOrDefault(name, ArrayBuffer())
      buffer += new MLSQLExternalStreamListener(item)
      _listenerStore.put(name, buffer)

    }
  }

  def removeListener(uuid: String) = {
    _listenerStore.asScala.foreach { items =>
      items._2.filter(f => f.item.uuid == uuid).headOption match {
        case Some(removeItem) => items._2.remove(items._2.indexOf(removeItem))
        case None =>
      }
    }
  }

  def addStore(job: MLSQLJobInfo) = {
    store.put(job.groupId, job)
  }

  def removeStore(groupId: String) = {
    store.remove(groupId)
  }

  def getJob(groupId: String) = {
    store.asScala.get(groupId)
  }

  def start(sparkSession: SparkSession) = {
    logInfo("Start streaming job monitor....")
    sparkSession.streams.addListener(new MLSQLStreamingQueryListener)
  }

  def close = {
    store.clear()
  }

  def isStream = {
    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.env().contains("streamName")
  }
}

case class MLSQLStreamListenerItem(uuid: String, user: String, streamName: String, eventName: MLSQLStreamEventName.eventName, handleHttpUrl: String, method: String, params: Map[String, String])

object MLSQLStreamEventName extends Enumeration {
  type eventName = Value
  val started = Value("started")
  val progress = Value("progress")
  val terminated = Value("terminated")
}

class MLSQLExternalStreamListener(val item: MLSQLStreamListenerItem) extends Logging with WowLog {
  val connectTimeout = 10 * 1000
  val socketTimeout = 10 * 1000

  def send(newParams: Map[String, String]) = {
    MLSQLStreamManager.executors.execute(new Runnable {
      override def run(): Unit = {
        try {
          _send(newParams ++ Map("eventName" -> item.eventName.toString))
        } catch {
          case e: Exception =>
            logInfo(format("Report stream fail:\n"))
            logInfo(format_cause(e))
        }

      }
    })
  }

  private def _send(newParams: Map[String, String]) = {
    HttpClientCrawler.requestByMethod(item.handleHttpUrl, item.method, item.params ++ newParams)
  }
}


class MLSQLStreamingQueryListener extends StreamingQueryListener with Logging with WowLog {

  def sync(name: String, id: String) = {
    // first we should check by name, since before the stream is really stared, we have record the name in
    // StreamingproJobManager
    JobManager.getJobInfo.filter(f => f._2.jobType == MLSQLJobType.STREAM
      && (f._2.jobName == name)).headOption match {
      case Some(job) =>
        if (job._2.groupId != id) {
          logInfo(format(
            s"""
               |JobManager:${job._2.jobName}
               |Spark streams: ${name}
               |Action: sync
               |Reason:: Job is not synced before.
             """.stripMargin))
          //onQueryStarted is stared before we acquire info from StreamingQuery
          JobManager.addJobManually(job._2.copy(groupId = id))
        }
      case None =>
        // we only care when stream is restore from ck without MLSQL instance restart
        // restore from  StreamManager.store
        MLSQLStreamManager.getJob(id) match {
          case Some(job) =>
            logInfo(format(
              s"""
                 |JobManager:${job.jobName}
                 |Spark streams: ${name}
                 |Action: sync
                 |Reason:: Job is not in JobManager but in MLSQLStreamManager.
             """.stripMargin))
            JobManager.addJobManually(job)
          case None =>
            // this  should not happen,throw exception
            throw new RuntimeException(s"MLSQL have unsync stream: ${name}")
        }
    }
  }

  def getJob(groupId: String) = {
    JobManager.getJobInfo.filter(f => f._2.groupId == groupId).map(f => f._2).headOption
  }

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    sync(event.name, event.id.toString)
    getJob(event.id.toString).headOption match {
      case Some(job) =>
        MLSQLStreamManager.runEvent(MLSQLStreamEventName.started, job.jobName, p => {
          p.send(Map("streamName" -> job.jobName, "jsonContent" -> "{}"))
        })
      case None => logError(format(s"Stream job [${event.id.toString}] is started. But we can not found it in JobManager."))

    }


  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val id = event.progress.id.toString
    sync(event.progress.name, id)

    getJob(event.progress.id.toString).headOption match {
      case Some(job) =>
        MLSQLStreamManager.runEvent(MLSQLStreamEventName.progress, job.jobName, p => {
          p.send(Map("streamName" -> job.jobName, "jsonContent" -> event.progress.json))
        })
      case None => logError(format(s"Stream job [${id}] is running. But we can not found it in JobManager."))

    }

  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    val id = event.id.toString
    var uuids = new ArrayBuffer[String]()
    getJob(id).headOption match {
      case Some(job) =>
        MLSQLStreamManager.runEvent(MLSQLStreamEventName.terminated, job.jobName, p => {
          p.send(Map("streamName" -> job.jobName, "jsonContent" -> "{}"))
        })
        MLSQLStreamManager.listeners().asScala.get(job.owner).map { items =>
          items.filter(p => p.item.streamName == job.jobName).map(
            f => uuids += f.item.uuid
          )
        }
      case None => logError(format(s"Stream job [${id}] is terminated. But we can not found it in JobManager."))
    }

    uuids.foreach(MLSQLStreamManager.removeListener)

    MLSQLStreamManager.removeStore(id)
    JobManager.getJobInfo.filter(f => f._2.jobType == MLSQLJobType.STREAM
      && f._2.groupId == id).headOption match {
      case Some(job) =>
        JobManager.removeJobManually(job._1)
      case None =>
    }
  }
}
