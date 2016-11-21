package streaming.core

import java.util.{Map => JMap}

import serviceframework.dispatcher.StrategyDispatcher
import streaming.common.DefaultShortNameMapping
import streaming.core.strategy.platform.{SparkStructuredStreamingRuntime, PlatformManager, SparkRuntime, SparkStreamingRuntime}

import scala.collection.JavaConversions._

/**
 * 5/2/16 WilliamZhu(allwefantasy@gmail.com)
 */
object Dispatcher {
  def dispatcher(contextParams: JMap[Any, Any]): StrategyDispatcher[Any] = {
    val defaultShortNameMapping = new DefaultShortNameMapping()
    if (contextParams!=null && contextParams.containsKey("streaming.job.file.path")) {
      val runtime = contextParams.get("_runtime_")

      val sparkContext = runtime match {
        case s: SparkStreamingRuntime => s.streamingContext.sparkContext
        case s2: SparkRuntime => s2.sparkContext
        case s3: SparkStructuredStreamingRuntime => s3.sparkSession.sparkContext
      }

      val jobFilePath = contextParams.get("streaming.job.file.path").toString

      var jobConfigStr = "{}"

      if (jobFilePath.startsWith("classpath://")) {
        val cleanJobFilePath = jobFilePath.substring("classpath://".length)
        jobConfigStr = scala.io.Source.fromInputStream(
          Dispatcher.getClass.getResourceAsStream(cleanJobFilePath)).getLines().
          mkString("\n")
      } else {
        jobConfigStr = sparkContext.
          textFile(jobFilePath).collect().mkString("\n")
      }

      StrategyDispatcher.getOrCreate(jobConfigStr,defaultShortNameMapping)
    } else {
      StrategyDispatcher.getOrCreate(null,defaultShortNameMapping)
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
