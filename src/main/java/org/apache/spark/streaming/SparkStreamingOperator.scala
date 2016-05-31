package org.apache.spark.streaming

import java.util.Map

import _root_.kafka.common.TopicAndPartition
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.DirectKafkaInputDStream
import streaming.core.strategy.platform.{DirectKafkaRecoverSource, RuntimeOperator, SparkStreamingRuntime, TestInputStreamRecoverSource}

import scala.collection.mutable.ArrayBuffer

/**
 * 5/3/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SparkStreamingOperator(_ssr: SparkStreamingRuntime) extends RuntimeOperator {
  val ssr = _ssr
  val ssc = ssr.streamingContext

  def inputStreamId(index: Int) = {
    ssc.graph.getInputStreams()(index).id
  }

  def directKafkaRecoverSource = {
    new DirectKafkaRecoverSource(this)
  }

  def testInputRecoverSource = {
    new TestInputStreamRecoverSource(this)
  }


  def inputDStreams = {
    ssc.graph.getInputStreams()
  }

  def inputTrackerMeta(time: Time) = {
    ssc.scheduler.inputInfoTracker.getInfo(time)
  }

  def directKafkaDStreamsMap = {
    inputDStreams.filter { is =>
      is.isInstanceOf[DirectKafkaInputDStream[_, _, _, _, _]]
    }.map(f => f.id).toSet
  }


  def isStreamingCanStop() = {
    ssc.scheduler.getPendingTimes().size == 0
  }

  def snapShotInputStreamState() = {
    val buffer = new ArrayBuffer[(Int, Any)]()
    ssc.graph.getInputStreams().foreach { inputDStream =>
      inputDStream match {
        case dkid: DirectKafkaInputDStream[_, _, _, _, _] =>
          val field = classOf[DirectKafkaInputDStream[_, _, _, _, _]].getDeclaredField("currentOffset")
          field.setAccessible(true)
          val currentOffset = field.get(dkid).asInstanceOf[Map[TopicAndPartition, Long]]
          buffer += ((dkid.id, currentOffset))
        case test: TestInputStream[_] =>
          val field = classOf[TestInputStream[_]].getDeclaredField("currentOffset")
          field.setAccessible(true)
          val currentOffset = field.get(test).asInstanceOf[Int]
          buffer += ((test.id, currentOffset))
        case _ =>
      }

    }
    buffer.toMap
  }

  def setInputStreamState(inputId: Int, state: Any) = {
    val inputDStream = ssc.graph.getInputStreams().filter(f => f.id == inputId).head
    inputDStream match {
      case dkid: DirectKafkaInputDStream[_, _, _, _, _] =>
        val field = classOf[DirectKafkaInputDStream[_, _, _, _, _]].getDeclaredField("currentOffsets")
        field.setAccessible(true)
        field.set(dkid, state)
      case test: TestInputStream[_] =>
        val field = classOf[TestInputStream[_]].getDeclaredField("currentOffset")
        field.setAccessible(true)
        field.set(test, state)
      case _ =>
    }
  }

  def setInputStreamState(inputDStream: InputDStream[(String, String)], state: Any) = {
    inputDStream match {
      case dkid: DirectKafkaInputDStream[_, _, _, _, _] =>
        val field = classOf[DirectKafkaInputDStream[_, _, _, _, _]].getDeclaredField("currentOffsets")
        field.setAccessible(true)
        field.set(dkid, state)
      case test: TestInputStream[_] =>
        val field = classOf[TestInputStream[_]].getDeclaredField("currentOffset")
        field.setAccessible(true)
        field.set(test, state)
      case _ =>
    }
  }
}
