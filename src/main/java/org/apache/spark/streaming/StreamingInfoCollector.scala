package org.apache.spark.streaming

import java.util.Map

import _root_.kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.DirectKafkaInputDStream

import scala.collection.mutable.ArrayBuffer

/**
 * 5/3/16 WilliamZhu(allwefantasy@gmail.com)
 */
class StreamingInfoCollector {
  def inputStreamId(ssc: StreamingContext,index: Int) = {
    ssc.graph.getInputStreams()(index).id
  }

  def snapShotInputStreamState(ssc: StreamingContext) = {
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

  def setInputStreamState(ssc: StreamingContext,inputId: Int, state: Any) = {
    val inputDStream = ssc.graph.getInputStreams().filter(f => f.id == inputId).head
    inputDStream match {
      case dkid: DirectKafkaInputDStream[_, _, _, _, _] =>
        val field = classOf[DirectKafkaInputDStream[_, _, _, _, _]].getDeclaredField("currentOffset")
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
