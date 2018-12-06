package streaming.core

import java.util.{Map => JMap}

import org.apache.http.client.fluent.Request
import serviceframework.dispatcher.StrategyDispatcher
import streaming.common.{DefaultShortNameMapping, HDFSOperator}
import streaming.core.strategy.platform.{PlatformManager, StreamingRuntime}

import scala.collection.JavaConversions._

/**
  * 5/2/16 WilliamZhu(allwefantasy@gmail.com)
  */
object Dispatcher {
  def dispatcher(contextParams: JMap[Any, Any]): StrategyDispatcher[Any] = {
    val defaultShortNameMapping = new DefaultShortNameMapping()
    if (contextParams != null && contextParams.containsKey("streaming.job.file.path")) {
      val runtime = contextParams.get("_runtime_").asInstanceOf[StreamingRuntime]


      val jobFilePath = contextParams.get("streaming.job.file.path").toString

      var jobConfigStr = "{}"

      if (jobFilePath.toLowerCase().startsWith("classpath://")) {
        val cleanJobFilePath = jobFilePath.substring("classpath://".length)
        jobConfigStr = scala.io.Source.fromInputStream(
          Dispatcher.getClass.getResourceAsStream(cleanJobFilePath)).getLines().
          mkString("\n")
      } else if (jobFilePath.toLowerCase().startsWith("http://") || jobFilePath.toLowerCase().startsWith("https://")) {
        jobConfigStr = Request.Get(jobFilePath)
          .connectTimeout(30000)
          .socketTimeout(30000)
          .execute().returnContent().asString();
      }
      else {
        jobConfigStr = HDFSOperator.readFile(jobFilePath)
      }

      if (jobConfigStr == null || jobConfigStr.isEmpty)
        jobConfigStr = "{}"

      StrategyDispatcher.getOrCreate(jobConfigStr, defaultShortNameMapping)
    } else {
      StrategyDispatcher.getOrCreate("{}", defaultShortNameMapping)
    }

  }

  def contextParams(jobName: String) = {
    val runtime = PlatformManager.getRuntime
    val tempParams: java.util.Map[Any, Any] = runtime.params
    val contextParams: java.util.HashMap[Any, Any] = new java.util.HashMap[Any, Any]()
    tempParams.foreach(f => contextParams += (f._1 -> f._2))
    contextParams.put("_client_", jobName)
    contextParams.put("_runtime_", runtime)
    contextParams
  }
}
