/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming

import java.util.Map

import _root_.kafka.common.TopicAndPartition
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.DirectKafkaInputDStream
import streaming.core.compositor.spark.streaming.ck.{DirectKafkaRecoverSource, TestInputStreamRecoverSource}
import streaming.core.strategy.platform.{RuntimeOperator, SparkStreamingRuntime}

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
