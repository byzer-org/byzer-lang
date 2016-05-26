package streaming.core

import java.util.{Map => JMap}

import serviceframework.dispatcher.StrategyDispatcher
import streaming.core.strategy.platform.{PlatformManager, SparkStreamingRuntime}

import scala.collection.JavaConversions._

/**
 * 5/2/16 WilliamZhu(allwefantasy@gmail.com)
 */
object Dispatcher {
  def dispatcher(contextParams: JMap[Any, Any]): StrategyDispatcher[Any] = {
    if (contextParams.containsKey("streaming.job.file.path")) {
      val runtime = contextParams.get("_runtime_").asInstanceOf[SparkStreamingRuntime]
      val jobConfigStr = runtime.streamingContext.sparkContext.
        textFile(contextParams.get("streaming.job.file.path").toString).collect().mkString("\n")
      StrategyDispatcher.getOrCreate(jobConfigStr)
    } else {
      StrategyDispatcher.getOrCreate(null)
    }

  }

  def contextParams(name: String) = {
    val runtime = PlatformManager.getRuntime(null, new java.util.HashMap[Any, Any]())
    val tempParams = runtime.params
    val contextParams: java.util.HashMap[Any, Any] = new java.util.HashMap[Any, Any]()
    tempParams.foreach(f => contextParams += (f._1 -> f._2))
    contextParams.put("_client_", name)
    contextParams.put("_runtime_", runtime)
    contextParams
  }
}
