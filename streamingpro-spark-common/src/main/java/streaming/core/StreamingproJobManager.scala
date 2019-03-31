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

package streaming.core

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by allwefantasy on 15/5/2017.
  */
object StreamingproJobManager {
  val logger = Logger.getLogger(classOf[StreamingproJobManager])
  private[this] var _jobManager: StreamingproJobManager = _
  private[this] val _executor = Executors.newFixedThreadPool(100)

  def shutdown = {
    _executor.shutdownNow()
    _jobManager.shutdown
    _jobManager = null
  }

  def init(spark: SparkSession, initialDelay: Long = 30, checkTimeInterval: Long = 5) = {
    synchronized {
      if (_jobManager == null) {
        logger.info(s"JobCanceller Timer  started with initialDelay=${initialDelay} checkTimeInterval=${checkTimeInterval}")
        _jobManager = new StreamingproJobManager(spark, initialDelay, checkTimeInterval)
        _jobManager.run
      }
    }
  }

  def initForTest(spark: SparkSession, initialDelay: Long = 30, checkTimeInterval: Long = 5) = {
    if (_jobManager == null) {
      logger.info(s"JobCanceller Timer  started with initialDelay=${initialDelay} checkTimeInterval=${checkTimeInterval}")
      _jobManager = new StreamingproJobManager(spark, initialDelay, checkTimeInterval)
    }
  }

  def run(session: SparkSession, job: StreamingproJobInfo, f: () => Unit): Unit = {
    try {
      if (_jobManager == null) {
        f()
      } else {
        session.sparkContext.setJobGroup(job.groupId, job.jobName, true)
        _jobManager.groupIdToStringproJobInfo.put(job.groupId, job)
        f()
      }
    } finally {
      handleJobDone(job.groupId)
      session.sparkContext.clearJobGroup()
    }
  }

  def asyncRun(session: SparkSession, job: StreamingproJobInfo, f: () => Unit) = {
    // TODO: (fchen) 改成callback
    _executor.execute(new Runnable {
      override def run(): Unit = {
        try {
          _jobManager.groupIdToStringproJobInfo.put(job.groupId, job)
          session.sparkContext.setJobGroup(job.groupId, job.jobName, true)
          f()
        } finally {
          handleJobDone(job.groupId)
          session.sparkContext.clearJobGroup()
        }
      }
    })
  }

  def getStreamingproJobInfo(owner: String,
                             jobType: String,
                             jobName: String,
                             jobContent: String,
                             timeout: Long): StreamingproJobInfo = {
    val startTime = System.currentTimeMillis()
    val groupId = _jobManager.nextGroupId.incrementAndGet().toString
    StreamingproJobInfo(owner, jobType, jobName, jobContent, groupId, startTime, timeout)
  }

  def getJobInfo: Map[String, StreamingproJobInfo] =
    _jobManager.groupIdToStringproJobInfo.asScala.toMap

  def addJobManually(job: StreamingproJobInfo) = {
    _jobManager.groupIdToStringproJobInfo.put(job.groupId, job)
  }

  def removeJobManually(groupId: String) = {
    handleJobDone(groupId)
  }

  def killJob(groupId: String): Unit = {
    _jobManager.cancelJobGroup(groupId)
  }

  private def handleJobDone(groupId: String): Unit = {
    _jobManager.groupIdToStringproJobInfo.remove(groupId)

  }
}

class StreamingproJobManager(spark: SparkSession, initialDelay: Long, checkTimeInterval: Long) {
  val groupIdToStringproJobInfo = new ConcurrentHashMap[String, StreamingproJobInfo]()
  val nextGroupId = new AtomicInteger(0)
  val logger = Logger.getLogger(classOf[StreamingproJobManager])
  val executor = Executors.newSingleThreadScheduledExecutor()

  def run = {
    executor.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        groupIdToStringproJobInfo.foreach { f =>
          val elapseTime = System.currentTimeMillis() - f._2.startTime
          if (f._2.timeout > 0 && elapseTime >= f._2.timeout) {
            cancelJobGroup(f._1)
          }
        }
      }
    }, initialDelay, checkTimeInterval, TimeUnit.SECONDS)
  }

  def cancelJobGroup(groupId: String): Unit = {
    logger.info("JobCanceller Timer cancel job group " + groupId)
    val job = groupIdToStringproJobInfo.get(groupId)
    if (job != null && job.jobType == StreamingproJobType.STREAM) {
      spark.streams.active.filter(f => f.id.toString == job.groupId).map(f => f.runId.toString).headOption match {
        case Some(_) => spark.streams.get(job.groupId).stop()
        case None => logger.warn(s"the stream job: ${job.groupId}, name:${job.jobName} is not in spark.streams.")
      }

    } else {
      spark.sparkContext.cancelJobGroup(groupId)
    }
    groupIdToStringproJobInfo.remove(groupId)
  }

  def shutdown = {
    executor.shutdownNow()
  }
}

case object StreamingproJobType {
  val SCRIPT = "script"
  val SQL = "sql"
  val STREAM = "stream"
}

case class StreamingproJobInfo(
                                owner: String,
                                jobType: String,
                                jobName: String,
                                jobContent: String,
                                groupId: String,
                                startTime: Long,
                                timeout: Long)
