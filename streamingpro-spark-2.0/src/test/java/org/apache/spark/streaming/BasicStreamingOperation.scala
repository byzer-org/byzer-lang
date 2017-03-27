package org.apache.spark.streaming

import org.apache.spark.util.ManualClock
import org.scalatest._
import serviceframework.dispatcher.{Compositor, StrategyDispatcher}
import streaming.common.ParamsUtil
import streaming.core.common.SQLContextHolder
import streaming.core.strategy.platform.{PlatformManager, SparkStructuredStreamingRuntime}

/**
  * 8/29/16 WilliamZhu(allwefantasy@gmail.com)
  */
trait BasicStreamingOperation extends FlatSpec with Matchers {

  def manualClock(streamingContext: StreamingContext) = {
    streamingContext.scheduler.clock.asInstanceOf[ManualClock]
  }

  def withStreamingContext[R](runtime: SparkStructuredStreamingRuntime)(block: SparkStructuredStreamingRuntime => R): R = {
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


  def getCompositorParam(item: Compositor[_]) = {
    val field = item.getClass.getDeclaredField("_configParams")
    field.setAccessible(true)
    field.get(item).asInstanceOf[java.util.List[java.util.Map[Any, Any]]]
  }


  def setupStreamingContext(streamingParams: Array[String], configFilePath: String) = {
    val extraParam = Array("-streaming.job.file.path", configFilePath)
    val params = new ParamsUtil(streamingParams ++ extraParam)
    PlatformManager.getOrCreate.run(params, false)
    val runtime = PlatformManager.getRuntime.asInstanceOf[SparkStructuredStreamingRuntime]
    runtime
  }

}
