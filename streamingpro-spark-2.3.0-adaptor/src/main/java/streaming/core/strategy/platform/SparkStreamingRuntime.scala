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

package streaming.core.strategy.platform

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import java.util.{List => JList, Map => JMap}

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
 * 4/27/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SparkStreamingRuntime(_params: JMap[Any, Any]) extends StreamingRuntime with PlatformManagerListener {

  self =>


  def name = "SPARK_STREAMING"

  var streamingContext: StreamingContext = createRuntime

  streamingContext.addStreamingListener(new BatchStreamingListener(this))


  private var _streamingRuntimeInfo: SparkStreamingRuntimeInfo = new SparkStreamingRuntimeInfo(this)

  override def streamingRuntimeInfo = _streamingRuntimeInfo

  override def resetRuntimeOperator(runtimeOperator: RuntimeOperator) = {
    _streamingRuntimeInfo.sparkStreamingOperator = new SparkStreamingOperator(this)
  }

  override def configureStreamingRuntimeInfo(streamingRuntimeInfo: StreamingRuntimeInfo) = {
    _streamingRuntimeInfo = streamingRuntimeInfo.asInstanceOf[SparkStreamingRuntimeInfo]
  }

  override def params = _params

  def createRuntime = {

    val conf = new SparkConf()
    params.filter(f => f._1.toString.startsWith("spark.")).foreach { f =>
      conf.set(f._1.toString, f._2.toString)
    }
    if (params.containsKey("streaming.master")) {
      conf.setMaster(params.get("streaming.master").toString)
    }
    conf.setAppName(params.get("streaming.name").toString)
    val duration = params.getOrElse("streaming.duration", "10").toString.toInt

    params.filter(f => f._1.toString.startsWith("streaming.spark.")).foreach { f =>
      val key = f._1.toString
      conf.set(key.substring("streaming".length + 1), f._2.toString)
    }

    val sessionBuilder = SparkSession.builder.config(conf)
    if (params.containsKey("streaming.enableHiveSupport") &&
      params.get("streaming.enableHiveSupport").toString.toBoolean) {
      sessionBuilder.enableHiveSupport()
    }
    val sparkSession = sessionBuilder.getOrCreate()
    params.put("_session_", sparkSession)

    val ssc = if (params.containsKey("streaming.checkpoint")) {
      val checkpoinDir = params.get("streaming.checkpoint").toString
      StreamingContext.getActiveOrCreate(checkpoinDir,
        () => {
          val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(duration))
          ssc.checkpoint(checkpoinDir)
          ssc
        }
      )
    } else {
      new StreamingContext(sparkSession.sparkContext, Seconds(duration))
    }
    ssc

  }


  override def destroyRuntime(stopGraceful: Boolean, stopSparkContext: Boolean = false) = {

    //clear inputDStreamId
    _streamingRuntimeInfo.jobNameToInputStreamId.clear()
    streamingContext.stop(stopSparkContext, stopGraceful)
    SparkStreamingRuntime.clearLastInstantiatedContext()
    SparkStreamingRuntime.sparkContext.set(null)
    true
  }

  override def startRuntime = {

    streamingRuntimeInfo.jobNameToInputStreamId.foreach { f =>
      _streamingRuntimeInfo.sparkStreamingOperator.directKafkaRecoverSource.restoreJobSate(f._1)
      _streamingRuntimeInfo.sparkStreamingOperator.testInputRecoverSource.restoreJobSate(f._1)
    }

    streamingContext.start()
    _streamingRuntimeInfo.jobNameToState.clear()
    this
  }

  override def awaitTermination = {
    streamingContext.awaitTermination()
  }

  override def startThriftServer: Unit = {

  }

  override def startHttpServer: Unit = {}

  override def processEvent(event: Event): Unit = {
    event match {
      case e: JobFlowGenerate =>
        val inputStreamId = streamingRuntimeInfo.sparkStreamingOperator.inputStreamId(e.index)
        streamingRuntimeInfo.jobNameToInputStreamId.put(e.jobName, inputStreamId)
      case _ =>
    }
  }

  SparkStreamingRuntime.setLastInstantiatedContext(self)


}

class SparkStreamingRuntimeInfo(ssr: SparkStreamingRuntime) extends StreamingRuntimeInfo {
  val jobNameToInputStreamId = new ConcurrentHashMap[String, Int]()
  val jobNameToState = new ConcurrentHashMap[String, Any]()
  var lastTime: Time = _
  var sparkStreamingOperator: SparkStreamingOperator = new SparkStreamingOperator(ssr)
}

class BatchStreamingListener(runtime: SparkStreamingRuntime) extends StreamingListener {

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val time = batchCompleted.batchInfo.batchTime
    val operator = runtime.streamingRuntimeInfo.sparkStreamingOperator

    //first check kafka offset which on direct approach, later we will add more sources
    operator.directKafkaRecoverSource.saveJobSate(time)
    operator.testInputRecoverSource.saveJobSate(time)

    runtime.streamingRuntimeInfo.lastTime = time
  }

}

object SparkStreamingRuntime {

  var sparkContext = new AtomicReference[SparkContext]()

  private val INSTANTIATION_LOCK = new Object()

  /**
   * Reference to the last created SQLContext.
   */
  @transient private val lastInstantiatedContext = new AtomicReference[SparkStreamingRuntime]()

  /**
   * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
   * This function can be used to create a singleton SQLContext object that can be shared across
   * the JVM.
   */
  def getOrCreate(params: JMap[Any, Any]): SparkStreamingRuntime = {
    INSTANTIATION_LOCK.synchronized {
      if (lastInstantiatedContext.get() == null) {
        new SparkStreamingRuntime(params)
      }
    }
    PlatformManager.getOrCreate.register(lastInstantiatedContext.get())
    lastInstantiatedContext.get()
  }

  private[platform] def clearLastInstantiatedContext(): Unit = {
    INSTANTIATION_LOCK.synchronized {
      PlatformManager.getOrCreate.unRegister(lastInstantiatedContext.get())
      lastInstantiatedContext.set(null)
    }
  }

  private[platform] def setLastInstantiatedContext(sparkStreamingRuntime: SparkStreamingRuntime): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(sparkStreamingRuntime)
    }
  }
}