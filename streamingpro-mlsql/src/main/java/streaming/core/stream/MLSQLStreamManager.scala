package streaming.core.stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import streaming.core.{StreamingproJobInfo, StreamingproJobManager, StreamingproJobType}
import streaming.log.Logging

import scala.collection.JavaConverters._

/**
  * 2019-01-21 WilliamZhu(allwefantasy@gmail.com)
  */
object MLSQLStreamManager {
  private val store = new java.util.concurrent.ConcurrentHashMap[String, StreamingproJobInfo]()

  def addStore(job: StreamingproJobInfo) = {
    store.put(job.groupId, job)
  }

  def getJob(groupId: String) = {
    store.asScala.get(groupId)
  }

  def start(sparkSession: SparkSession) = {
    sparkSession.streams.addListener(new MLSQLStreamingQueryListener)
  }

  def close = {

  }
}

class MLSQLStreamingQueryListener extends StreamingQueryListener with Logging {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    // first we should check by name, since before the stream is really stared, we have record the name in
    // StreamingproJobManager
    StreamingproJobManager.getJobInfo.filter(f => f._2.jobType == StreamingproJobType.STREAM
      && (f._2.jobName == event.name)).headOption match {
      case Some(job) =>
        if (job._2.groupId != event.id) {
          //onQueryStarted is stared before we acquire info from StreamingQuery
          StreamingproJobManager.addJobManually(job._2.copy(groupId = event.id.toString))
        }
      case None =>
        // we only care when stream is restore from ck without MLSQL instance restart
        // restore from  StreamManager.store
        MLSQLStreamManager.getJob(event.id.toString) match {
          case Some(job) => StreamingproJobManager.addJobManually(job)
          case None =>
            // this  should not happen,throw exception
            throw new RuntimeException(s"MLSQL have unsync stream: ${event.name}")
        }
    }

  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    // do nothing for now
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    StreamingproJobManager.getJobInfo.filter(f => f._2.jobType == StreamingproJobType.STREAM
      && f._2.groupId == event.id.toString).headOption match {
      case Some(job) => StreamingproJobManager.removeJobManually(job._1)
      case None =>
    }
  }
}
