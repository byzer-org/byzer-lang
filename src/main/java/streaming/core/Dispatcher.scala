package streaming.core

import java.util

import serviceframework.dispatcher.StrategyDispatcher
import streaming.core.strategy.platform.PlatformManager

import scala.collection.JavaConversions._

/**
 * 5/2/16 WilliamZhu(allwefantasy@gmail.com)
 */
object Dispatcher {
  def dispatcher: StrategyDispatcher[Any] = {
    StrategyDispatcher.getOrCreate()
  }

  def contextParams(name:String) = {
    val runtime = PlatformManager.getRuntime(null, new java.util.HashMap[Any, Any]())
    val tempParams = runtime.params
    val contextParams: util.HashMap[Any, Any] = new java.util.HashMap[Any, Any]()
    tempParams.foreach(f => contextParams += (f._1 -> f._2))
    contextParams.put("_client_", name)
    contextParams.put("_runtime_", runtime)
    contextParams
  }
}
