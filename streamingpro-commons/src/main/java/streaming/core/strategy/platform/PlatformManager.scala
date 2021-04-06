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

import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.{Map => JMap}

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Application
import net.csdn.common.logging.Loggers
import serviceframework.dispatcher.StrategyDispatcher
import streaming.common.zk.{ZKClient, ZkRegister}
import streaming.core.strategy.JobStrategy
import streaming.core.{Dispatcher, StreamingApp}
import tech.mlsql.common.utils.base.TryTool
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.shell.command.ParamsUtil
import tech.mlsql.runtime.MLSQLPlatformLifecycle

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * 4/27/16 WilliamZhu(allwefantasy@gmail.com)
 */


class PlatformManager extends Logging{
  self =>
  val config = new AtomicReference[ParamsUtil]()

  def findDispatcher(contextParams: JMap[Any, Any]): StrategyDispatcher[Any] = {
    Dispatcher.dispatcher(contextParams)
  }

  def findDispatcher: StrategyDispatcher[Any] = {
    Dispatcher.dispatcher(Dispatcher.contextParams(""))
  }

  val listeners = new ArrayBuffer[PlatformManagerListener]()

  def register(listener: PlatformManagerListener) = {
    listeners += listener
  }

  def unRegister(listener: PlatformManagerListener) = {
    listeners -= listener
  }

  val lifeCycleCallback = ArrayBuffer[MLSQLPlatformLifecycle]()

  def registerMLSQLPlatformLifecycle(listener: MLSQLPlatformLifecycle) = {
    lifeCycleCallback += listener
  }

  def unRegisterMLSQLPlatformLifecycle(listener: MLSQLPlatformLifecycle) = {
    lifeCycleCallback -= listener
  }

  def startRestServer = {
    ServiceFramwork.scanService.setLoader(classOf[StreamingApp])
    ServiceFramwork.enableNoThreadJoin()
    Application.main(Array())
  }

  def startThriftServer(runtime: StreamingRuntime) = {
    runtime.startThriftServer
  }

  def registerToZk(params: ParamsUtil) = {
    zk = ZkRegister.registerToZk(params)
  }

  var zk: ZKClient = null


  def run(_params: ParamsUtil, reRun: Boolean = false) = {

    if (!reRun) {
      config.set(_params)
    }

    val params = config.get()

    val lastStreamingRuntimeInfo = if (reRun) {
      val tempRuntime = PlatformManager.getRuntime
      tempRuntime.getClass.getMethod("clearLastInstantiatedContext").invoke(null)
      Some(tempRuntime.streamingRuntimeInfo)
    } else None


    val tempParams = new java.util.HashMap[Any, Any]()
    params.getParamsMap.asScala.filter(f => f._1.startsWith("streaming.")).foreach { f => tempParams.put(f._1, f._2) }


    TryTool.tryLogNonFatalError {
      lifeCycleCallback.foreach(f => f.beforeRuntime(params.getParamsMap.asScala.toMap))
    }

    val runtime = PlatformManager.getRuntime

    TryTool.tryLogNonFatalError {
      lifeCycleCallback.foreach(f => f.afterRuntime(runtime, params.getParamsMap.asScala.toMap))
    }

    val dispatcher = findDispatcher

    var jobs: Array[String] = dispatcher.strategies.asScala.filter(f => f._2.isInstanceOf[JobStrategy]).keys.toArray

    if (params.hasParam("streaming.jobs"))
      jobs = params.getParam("streaming.jobs").split(",")

    lastStreamingRuntimeInfo match {
      case Some(ssri) =>
        runtime.configureStreamingRuntimeInfo(ssri)
        runtime.resetRuntimeOperator(null)
      case None =>
    }

    if (params.getBooleanParam("streaming.rest", false) && !reRun) {
      startRestServer
    }

    if (params.getBooleanParam("streaming.thrift", false)
      && !reRun
    ) {
      startThriftServer(runtime)
    }

    if (params.hasParam("streaming.zk.conf_root_dir") && !reRun) {
      registerToZk(params)
    }

    /*
        Once streaming.mode.application.fails_all is set true,
        Any job fails will result the others not be executed.
     */
    val failsAll = params.getBooleanParam("streaming.mode.application.fails_all", false)
    StrategyDispatcher.throwsException = failsAll

    val jobCounter = new AtomicInteger(0)

    TryTool.tryLogNonFatalError {
      lifeCycleCallback.foreach(f => f.beforeDispatcher(runtime, params.getParamsMap.asScala.toMap))
    }

    jobs.foreach {
      jobName =>
        /*
        todo: We should check if it runs on Yarn, it true, then
              convert the exception to Yarn exception otherwise the
              Yarn will show the status success even there are exceptions thrown
         */
        dispatcher.dispatch(Dispatcher.contextParams(jobName))
        val index = jobCounter.get()

        listeners.foreach { listener =>
          listener.processEvent(JobFlowGenerate(jobName, index, dispatcher.findStrategies(jobName).get.head))
        }
        jobCounter.incrementAndGet()
    }

    TryTool.tryLogNonFatalError {
      lifeCycleCallback.foreach(f => f.afterDispatcher(runtime, params.getParamsMap.asScala.toMap))
    }


    if (params.getBooleanParam("streaming.unitest.startRuntime", true)) {
      runtime.startRuntime
    }
    PlatformManager.RUNTIME_IS_READY.compareAndSet(false, true)
    if (params.getBooleanParam("streaming.unitest.awaitTermination", true)) {
      runtime.awaitTermination
    }
  }

  PlatformManager.setLastInstantiatedContext(self)
}

object PlatformManager {
  private val INSTANTIATION_LOCK = new Object()
  //SparkRuntime.RUNTIME_IS_READY.compareAndSet(false, true)
  val RUNTIME_IS_READY = new AtomicBoolean(false)

  /**
   * Reference to the last created SQLContext.
   */
  @transient private val lastInstantiatedContext = new AtomicReference[PlatformManager]()

  /**
   * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
   * This function can be used to create a singleton SQLContext object that can be shared across
   * the JVM.
   */
  def getOrCreate: PlatformManager = {
    INSTANTIATION_LOCK.synchronized {
      if (lastInstantiatedContext.get() == null) {
        new PlatformManager()
      }
    }
    lastInstantiatedContext.get()
  }

  private[platform] def clearLastInstantiatedContext(): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(null)
    }
  }

  private[platform] def setLastInstantiatedContext(platformManager: PlatformManager): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(platformManager)
    }
  }


  def createRuntimeByPlatform(name: String, tempParams: java.util.Map[Any, Any]) = {
    Class.forName(name).
      getMethod("getOrCreate", classOf[JMap[Any, Any]]).
      invoke(null, tempParams).asInstanceOf[StreamingRuntime]
  }


  def clear = {
    lastInstantiatedContext.set(null)
  }


  def getRuntime: StreamingRuntime = {
    val params: JMap[String, String] = getOrCreate.config.get().getParamsMap
    val tempParams: JMap[Any, Any] = new util.HashMap[Any, Any]()
    params.asScala.map(f => tempParams.put(f._1.asInstanceOf[Any], f._2.asInstanceOf[Any]))

    val platformName = params.get("streaming.platform")
    val runtime = createRuntimeByPlatform(platformNameMapping(platformName), tempParams)

    runtime
  }

  def SPAKR_STREAMING = "spark_streaming"

  def SPAKR_STRUCTURED_STREAMING = "spark_structured_streaming"

  def SPAKR_S_S = "ss"

  def STORM = "storm"

  def SPARK = "spark"

  def MLSQL = "mlsql"

  def FLINK_STREAMING = "flink_streaming"

  def platformNameMapping = Map[String, String](
    SPAKR_S_S -> "streaming.core.strategy.platform.SparkStructuredStreamingRuntime",
    SPAKR_STRUCTURED_STREAMING -> "streaming.core.strategy.platform.SparkStructuredStreamingRuntime",
    FLINK_STREAMING -> "streaming.core.strategy.platform.FlinkStreamingRuntime",
    SPAKR_STREAMING -> "streaming.core.strategy.platform.SparkStreamingRuntime",
    SPARK -> "streaming.core.strategy.platform.SparkRuntime",
    MLSQL -> "streaming.core.strategy.platform.MLSQLRuntime"
  )

}

