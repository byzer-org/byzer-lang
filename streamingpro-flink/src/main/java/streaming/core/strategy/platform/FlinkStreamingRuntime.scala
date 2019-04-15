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

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Created by allwefantasy on 20/3/2017.
  */
class FlinkStreamingRuntime(_params: JMap[Any, Any]) extends StreamingRuntime with PlatformManagerListener {
  self =>

  def name = "FlinkStreaming"

  val runtime = createRuntime

  def createRuntime = {
    StreamExecutionEnvironment.getExecutionEnvironment
  }

  override def startRuntime: StreamingRuntime = {
    runtime.execute(_params.get("streaming.name").toString)
    this
  }

  override def destroyRuntime(stopGraceful: Boolean, stopContext: Boolean): Boolean = false

  override def streamingRuntimeInfo: StreamingRuntimeInfo = null

  override def resetRuntimeOperator(runtimeOperator: RuntimeOperator): Unit = {}

  override def configureStreamingRuntimeInfo(streamingRuntimeInfo: StreamingRuntimeInfo): Unit = {}

  override def awaitTermination: Unit = {}

  override def params: JMap[Any, Any] = _params

  override def processEvent(event: Event): Unit = {}

  FlinkStreamingRuntime.setLastInstantiatedContext(this)

  override def startThriftServer: Unit = {}

  override def startHttpServer: Unit = {}
}

object FlinkStreamingRuntime {


  private val INSTANTIATION_LOCK = new Object()

  /**
    * Reference to the last created SQLContext.
    */
  @transient private val lastInstantiatedContext = new AtomicReference[FlinkStreamingRuntime]()

  /**
    * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
    * This function can be used to create a singleton SQLContext object that can be shared across
    * the JVM.
    */
  def getOrCreate(params: JMap[Any, Any]): FlinkStreamingRuntime = {
    INSTANTIATION_LOCK.synchronized {
      if (lastInstantiatedContext.get() == null) {
        new FlinkStreamingRuntime(params)
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

  private[platform] def setLastInstantiatedContext(flinkStreamingRuntime: FlinkStreamingRuntime): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(flinkStreamingRuntime)
    }
  }
}




