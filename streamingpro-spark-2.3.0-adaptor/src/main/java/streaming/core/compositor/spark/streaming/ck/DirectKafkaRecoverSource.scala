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

package streaming.core.compositor.spark.streaming.ck

import java.text.SimpleDateFormat
import java.util.Date

import kafka.common.TopicAndPartition
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.{SparkStreamingOperator, StreamingContext, Time}
import tech.mlsql.common.utils.hdfs.HDFSOperator


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
          saveKafkaOffset(ssc, pathDir, f._1, f._2)
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
        ssr.streamingRuntimeInfo.jobNameToInputStreamId.filter(f => directKafkaMap.contains(f._2)).
          filter(f => f._1 == jobName).
          foreach { f =>
            val state = kafkaOffset(ssc, pathDir, f._1)
            if (state != null) {
              operator.setInputStreamState(f._2, state)
            }
          }
      case None =>
        ssr.streamingRuntimeInfo.jobNameToInputStreamId.filter(f => directKafkaMap.contains(f._2)).
          filter(f => f._1 == jobName).
          foreach { f =>
            val state = operator.ssr.streamingRuntimeInfo.jobNameToState.get(f._1)
            if (state != null) {
              operator.setInputStreamState(f._2, state)
            }
          }
    }
  }

  override def jobSate(time: Time) = {
    import scala.collection.JavaConversions._
    val info = operator.inputTrackerMeta(time)
    val directKafkaMap = operator.directKafkaDStreamsMap
    val jobNameToOffset = ssr.streamingRuntimeInfo.jobNameToInputStreamId.filter(f => directKafkaMap.contains(f._2)).
      map { f =>
        val offsetRange = info(f._2).metadata("offsets").asInstanceOf[List[OffsetRange]]
        val nextRoundOffsets = offsetRange.map(f => (f.topicAndPartition(), f.untilOffset)).toMap
        (f._1, nextRoundOffsets)
      }.toMap
    jobNameToOffset
  }


  def saveKafkaOffset(context: StreamingContext, path: String, suffix: String, offsets: Any) = {

    def getTime(pattern: String): String = {
      new SimpleDateFormat(pattern).format(new Date())
    }

    val fileSystem = FileSystem.get(context.sparkContext.hadoopConfiguration)

    if (!fileSystem.exists(new Path(path))) {
      fileSystem.mkdirs(new Path(path))
    }

    val item = getTime("yyyyMMddHHmmss") + "_" + suffix
    val res = offsets.asInstanceOf[Map[TopicAndPartition, Long]].map { or =>
      s"${or._1.topic},${or._1.partition},${or._2}"
    }.map(f => ("", f))
    HDFSOperator.saveFile(path, item, res.toIterator)
  }


  def kafkaOffset(context: StreamingContext, pathDir: String, suffix: String): Map[TopicAndPartition, Long] = {

    val fileSystem = FileSystem.get(context.sparkContext.hadoopConfiguration)

    if (!fileSystem.exists(new Path(pathDir))) {
      return null
    }

    val files = FileSystem.get(context.sparkContext.hadoopConfiguration).listStatus(new Path(pathDir)).toList
    if (files.length == 0) {
      return null
    }

    val jobFiles = files.filter(f => f.getPath.getName.endsWith("_" + suffix)).sortBy(f => f.getPath.getName).reverse
    if (jobFiles.length == 0) return null

    val restoreKafkaFile = jobFiles.head.getPath.getName

    val keepNum = if (operator.ssr.params.containsKey("streaming.kafka.offset.num")) operator.ssr.params.get("streaming.kafka.offset.num").toString.toInt
    else 1

    jobFiles.slice(keepNum, jobFiles.size).foreach { f =>
      fileSystem.delete(f.getPath, false)
    }


    val lines = context.sparkContext.textFile(pathDir + "/" + restoreKafkaFile).map { f =>
      val Array(topic, partition, from) = f.split(",")
      (topic, partition.toInt, from.toLong)
    }.collect().groupBy(f => f._1)

    val fromOffsets = lines.flatMap { topicPartitions =>
      topicPartitions._2.map { f =>
        (TopicAndPartition(f._1, f._2), f._3)
      }.toMap
    }
    fromOffsets


  }


}
