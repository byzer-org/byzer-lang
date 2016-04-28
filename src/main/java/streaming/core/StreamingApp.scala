package streaming.core

import java.util
import java.util.{List => JList, Map => JMap}

import net.csdn.common.settings.ImmutableSettings.settingsBuilder
import net.csdn.common.settings.Settings
import serviceframework.dispatcher.StrategyDispatcher
import streaming.common.ParamsUtil
import streaming.core.strategy.platform.PlatformManager

import scala.collection.JavaConversions._


object StreamingApp {

  private def dispatcher: StrategyDispatcher[Any] = {
    val setting: Settings = settingsBuilder.build()
    new StrategyDispatcher[Any](setting)
  }

  def main(args: Array[String]): Unit = {
    //获取启动参数
    val params = new ParamsUtil(args)
    require(params.hasParam("streaming.name"), "Application name should be set")
    var apps: Array[String] = dispatcher.strategies.keys().toArray
    if (params.hasParam("streaming.apps"))
      apps = params.getParam("streaming.apps").split(",")

    val tempParams = new java.util.HashMap[Any, Any]()
    params.getParamsMap.filter(f => f._1.startsWith("streaming.")).foreach { f => tempParams.put(f._1, f._2) }
    val runtime = PlatformManager.getRuntime(params.getParam("streaming.name"), tempParams)
    apps.foreach {
      appName =>
        val contextParams: util.HashMap[Any, Any] = new java.util.HashMap[Any, Any]()
        tempParams.foreach(f => contextParams += (f._1 -> f._2))
        contextParams.put("_client_", appName)
        contextParams.put("_runtime_", runtime)
        dispatcher.dispatch(contextParams)
    }
    runtime.awaitTermination
  }

}
