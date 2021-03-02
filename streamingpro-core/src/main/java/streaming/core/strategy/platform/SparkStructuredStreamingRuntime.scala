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

import java.util.concurrent.atomic.AtomicReference
import java.util.{Map => JMap}

import org.apache.log4j.Logger

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
 * 11/20/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SparkStructuredStreamingRuntime(_params: JMap[Any, Any]) extends StreamingRuntime with PlatformManagerListener  {
  self =>

  private val logger = Logger.getLogger(classOf[SparkStructuredStreamingRuntime])

  def name = "SPAKR_STRUCTURED_STREAMING"

  var sparkSession: SparkSession = createRuntime


  override def streamingRuntimeInfo = null

  override def resetRuntimeOperator(runtimeOperator: RuntimeOperator) = {

  }

  override def configureStreamingRuntimeInfo(streamingRuntimeInfo: StreamingRuntimeInfo) = {

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

    params.filter(f => f._1.toString.startsWith("streaming.spark.")).foreach { f =>
      val key = f._1.toString
      conf.set(key.substring("streaming".length + 1), f._2.toString)
    }
    val sparkSession = SparkSession.builder().config(conf)
    if (params.containsKey("streaming.enableHiveSupport") &&
      params.get("streaming.enableHiveSupport").toString.toBoolean) {
      sparkSession.enableHiveSupport()
    }
    sparkSession.getOrCreate()
  }

  params.put("_session_", sparkSession)

  override def startRuntime: StreamingRuntime = {
    this
  }

  override def awaitTermination: Unit = {}

  override def destroyRuntime(stopGraceful: Boolean, stopContext: Boolean): Boolean = {
    true
  }

  override def processEvent(event: Event): Unit = {}

  SparkStructuredStreamingRuntime.setLastInstantiatedContext(self)

  override def startThriftServer: Unit = {}

  override def startHttpServer: Unit = {}
}

object SparkStructuredStreamingRuntime {

  var sparkContext = new AtomicReference[SparkContext]()

  private val INSTANTIATION_LOCK = new Object()

  /**
   * Reference to the last created SQLContext.
   */
  @transient private val lastInstantiatedContext = new AtomicReference[SparkStructuredStreamingRuntime]()

  /**
   * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
   * This function can be used to create a singleton SQLContext object that can be shared across
   * the JVM.
   */
  def getOrCreate(params: JMap[Any, Any]): SparkStructuredStreamingRuntime = {
    INSTANTIATION_LOCK.synchronized {
      if (lastInstantiatedContext.get() == null) {
        new SparkStructuredStreamingRuntime(params)
      }
      PlatformManager.getOrCreate.register(lastInstantiatedContext.get())
    }
    lastInstantiatedContext.get()
  }

  private[platform] def clearLastInstantiatedContext(): Unit = {
    INSTANTIATION_LOCK.synchronized {
      PlatformManager.getOrCreate.unRegister(lastInstantiatedContext.get())
      lastInstantiatedContext.set(null)
    }
  }

  private[platform] def setLastInstantiatedContext(sparkStructuredStreamingRuntime: SparkStructuredStreamingRuntime): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(sparkStructuredStreamingRuntime)
    }
  }
}