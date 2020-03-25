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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.{SparkStreamingOperator, StreamingContext, TestInputStream, Time}
import tech.mlsql.common.utils.hdfs.HDFSOperator

/**
  * 5/9/16 WilliamZhu(allwefantasy@gmail.com)
  */
class TestInputStreamRecoverSource(operator: SparkStreamingOperator) extends SparkStreamingRecoverSource {
  val ssr = operator.ssr
  val ssc = operator.ssc


  override def saveJobSate(time: Time) = {

    jobSate(time).foreach { f =>
      recoverPath match {
        case Some(pathDir) =>
          saveStateToHDFS(ssc, pathDir, f._1, f._2)
        case None =>
          operator.ssr.streamingRuntimeInfo.jobNameToState.put(f._1, f._2)
      }

    }
  }

  override def recoverPath = {
    if (operator.ssr.params.containsKey("streaming.testinputstream.offsetPath")) {
      Some(ssr.params.get("streaming.testinputstream.offsetPath").toString)
    } else {
      None
    }
  }

  override def restoreJobSate(jobName: String) = {
    import scala.collection.JavaConversions._
    val directKafkaMap = testInputStreams
    recoverPath match {
      case Some(pathDir) =>
        ssr.streamingRuntimeInfo.jobNameToInputStreamId.filter(f => directKafkaMap.contains(f._2)).
          filter(f => f._1 == jobName).
          foreach { f =>
            val state = stateFromHDFS(ssc, pathDir, f._1)
            if (state != -1) {
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
    val directKafkaMap = testInputStreams
    val jobNameToOffset = ssr.streamingRuntimeInfo.jobNameToInputStreamId.filter(f => directKafkaMap.contains(f._2)).
      map { f =>
        val offset = info(f._2).metadata("offsets").asInstanceOf[Int]
        (f._1, offset + 1)
      }.toMap
    jobNameToOffset
  }

  def testInputStreams = {
    operator.inputDStreams.filter(is => is.isInstanceOf[TestInputStream[_]]).map(f => f.id).toSet
  }

  def saveStateToHDFS(context: StreamingContext, path: String, suffix: String, offsets: Any) = {

    def getTime(pattern: String): String = {
      new SimpleDateFormat(pattern).format(new Date())
    }

    val fileSystem = FileSystem.get(context.sparkContext.hadoopConfiguration)

    if (!fileSystem.exists(new Path(path))) {
      fileSystem.mkdirs(new Path(path))
    }

    val item = getTime("yyyyMMddHHmmss") + "_" + suffix
    val res = List(offsets.asInstanceOf[Int].toString).map(f => ("", f))
    HDFSOperator.saveFile(path, item, res.toIterator)
  }


  def stateFromHDFS(context: StreamingContext, pathDir: String, suffix: String): Int = {
    val fileSystem = FileSystem.get(context.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(new Path(pathDir))) {
      return -1
    }

    val files = fileSystem.listStatus(new Path(pathDir)).toList
    if (files.length == 0) {
      return -1
    }

    val jobFiles = files.filter(f => f.getPath.getName.endsWith("_" + suffix)).sortBy(f => f.getPath.getName).reverse
    if (jobFiles.length == 0) return -1

    val restoreKafkaFile = jobFiles.head.getPath.getName


    jobFiles.slice(1, jobFiles.size).foreach { f =>
      fileSystem.delete(f.getPath, false)
    }


    val line = context.sparkContext.textFile(pathDir + "/" + restoreKafkaFile).map { f =>
      f
    }.collect().head

    line.toInt
  }

}
