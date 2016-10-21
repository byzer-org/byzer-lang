package org.apache.spark.streaming

import org.apache.spark.util.ManualClock
import org.scalatest._
import serviceframework.dispatcher.StrategyDispatcher
import streaming.common.{ParamsUtil, SQLContextHolder}
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime, SparkStreamingRuntime}

/**
 * 8/29/16 WilliamZhu(allwefantasy@gmail.com)
 */
trait BasicStreamingOperation extends FlatSpec with Matchers {

  def manualClock(streamingContext: StreamingContext) = {
    streamingContext.scheduler.clock.asInstanceOf[ManualClock]
  }

  def withStreamingContext[R](runtime: SparkStreamingRuntime)(block: SparkStreamingRuntime => R): R = {
    try {
      block(runtime)
    } finally {
      try {
        StrategyDispatcher.clear
        PlatformManager.getOrCreate
        SQLContextHolder.sqlContextHolder.clear
        SQLContextHolder.sqlContextHolder = null
        runtime.destroyRuntime(false, true)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  def withBatchContext[R](runtime: SparkRuntime)(block: SparkRuntime => R): R = {
    try {
      block(runtime)
    } finally {
      try {
        StrategyDispatcher.clear
        PlatformManager.getOrCreate
        SQLContextHolder.sqlContextHolder.clear
        SQLContextHolder.sqlContextHolder = null
        runtime.destroyRuntime(false, true)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  def setupBatchContext(batchParams: Array[String], configFilePath: String) = {
    val extraParam = Array("-streaming.job.file.path", configFilePath)
    val params = new ParamsUtil(batchParams ++ extraParam)
    PlatformManager.getOrCreate.run(params, false)
    val runtime = PlatformManager.getRuntime.asInstanceOf[SparkRuntime]
    runtime
  }

  def setupStreamingContext(streamingParams: Array[String], configFilePath: String) = {
    val extraParam = Array("-streaming.job.file.path", configFilePath)
    val params = new ParamsUtil(streamingParams ++ extraParam)
    PlatformManager.getOrCreate.run(params, false)
    val runtime = PlatformManager.getRuntime.asInstanceOf[SparkStreamingRuntime]
    runtime
  }

}
