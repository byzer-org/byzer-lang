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

package com.hortonworks.spark.sql.kafka08

import kafka.common.TopicAndPartition
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

/**
  * Created by allwefantasy on 2/7/2017.
  */
private object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  /**
    * Read TopicPartitions from json string
    */
  def partitions(str: String): Array[TopicAndPartition] = {
    try {
      Serialization.read[Map[String, Seq[Int]]](str).flatMap { case (topic, parts) =>
        parts.map { part =>
          new TopicAndPartition(topic, part)
        }
      }.toArray
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":[0,1],"topicB":[0,1]}, got $str""")
    }
  }

  /**
    * Write TopicPartitions as json string
    */
  def partitions(partitions: Iterable[TopicAndPartition]): String = {
    val result = new HashMap[String, List[Int]]
    partitions.foreach { tp =>
      val parts: List[Int] = result.getOrElse(tp.topic, Nil)
      result += tp.topic -> (tp.partition :: parts)
    }
    Serialization.write(result)
  }

  /**
    * Read per-TopicPartition offsets from json string
    */
  def partitionOffsets(str: String): Map[TopicAndPartition, LeaderOffset] = {
    try {
      Serialization.read[Map[String, Map[Int, Seq[String]]]](str).flatMap { case (topic, partOffsets) =>
        partOffsets.map { case (part, offset) =>
          new TopicAndPartition(topic, part) -> LeaderOffset(
            offset(0),
            offset(1).toInt,
            offset(2).toLong)
        }
      }.toMap
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}, got $str""")
    }
  }

  /**
    * Write per-TopicPartition offsets as json string
    */
  def partitionOffsets(partitionOffsets: Map[TopicAndPartition, LeaderOffset]): String = {
    val result = new HashMap[String, HashMap[Int, Seq[String]]]()
    implicit val ordering = new Ordering[TopicPartition] {
      override def compare(x: TopicPartition, y: TopicPartition): Int = {
        Ordering.Tuple2[String, Int].compare((x.topic, x.partition), (y.topic, y.partition))
      }
    }
    partitionOffsets.foreach { tpAndLo =>
      val off = tpAndLo._2
      val tp = tpAndLo._1
      val parts = result.getOrElse(tp.topic, new HashMap[Int, Seq[String]])
      parts += tp.partition -> Seq(off.host, off.port.toString, off.offset.toString)
      result += tp.topic -> parts
    }
    Serialization.write(result)
  }
}
