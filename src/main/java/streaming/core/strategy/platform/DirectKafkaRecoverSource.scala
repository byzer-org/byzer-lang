package streaming.core.strategy.platform

import org.apache.spark.streaming.{SparkStreamingOperator, Time}
import streaming.core.compositor.spark.hdfs.HDFSOperator

/**
 * 5/9/16 WilliamZhu(allwefantasy@gmail.com)
 */
class DirectKafkaRecoverSource(operator: SparkStreamingOperator) extends SparkStreamingRecoverSource {
  val ssr = operator.ssr
  val ssc = operator.ssc

  override def saveJobSate(time: Time) = {
    jobSate(time).foreach { f =>
      recoverPath match {
        case Some(pathDir) =>
          HDFSOperator.saveKafkaOffset(ssc, pathDir, f._1, f._2)
        case None =>
          operator.ssr.streamingRuntimeInfo.jobNameToState.put(f._1, f._2)
      }

    }
  }


  override def recoverPath = {
    if (operator.ssr.params.containsKey("streaming.kafka.offsetPath")) {
      Some(ssr.params.get("streaming.kafka.offsetPath").toString)
    } else {
      None
    }
  }

  override def restoreJobSate(jobName: String) = {
    import scala.collection.JavaConversions._
    val directKafkaMap = operator.directKafkaDStreamsMap
    recoverPath match {
      case Some(pathDir) =>
        ssr.streamingRuntimeInfo.jobNameToInputStreamId.filter(f => directKafkaMap.containsKey(f._2)).
          filter(f => f._1 == jobName).
          foreach { f =>
          operator.setInputStreamState(f._2, HDFSOperator.kafkaOffset(ssc, pathDir, f._1))
        }
      case None =>
        ssr.streamingRuntimeInfo.jobNameToInputStreamId.filter(f => directKafkaMap.containsKey(f._2)).
          filter(f => f._1 == jobName).
          foreach { f =>
          val state = operator.ssr.streamingRuntimeInfo.jobNameToState.get(f._1)
          operator.setInputStreamState(f._2, state)
        }
    }
  }

  override def jobSate(time: Time) = {
    import scala.collection.JavaConversions._
    val info = operator.inputTrackerMeta(time)
    val directKafkaMap = operator.directKafkaDStreamsMap
    val jobNameToOffset = ssr.streamingRuntimeInfo.jobNameToInputStreamId.filter(f => directKafkaMap.containsKey(f._2)).
      map(f => (f._1, info(f._2).metadata("offsets"))).toMap
    jobNameToOffset
  }


}
