package org.apache.spark.streaming

import org.apache.spark.Logging
import org.apache.spark.util.ManualClock
import streaming.core.strategy.platform.SparkStreamingRuntime

/**
 * 8/29/16 WilliamZhu(allwefantasy@gmail.com)
 */
trait BasicStreamingOperation extends Logging{

  def manualClock(streamingContext: StreamingContext) = {
    streamingContext.scheduler.clock.asInstanceOf[ManualClock]
  }

  def withStreamingContext[R](runtime: SparkStreamingRuntime)(block: SparkStreamingRuntime => R): R = {
    try {
      block(runtime)
    } finally {
      try {
        runtime.destroyRuntime(false, true)
      } catch {
        case e: Exception =>
          logError("Error stopping StreamingContext", e)
      }
    }
  }
}
