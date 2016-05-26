package streaming.rest

import java.util.concurrent.atomic.AtomicInteger

import net.csdn.annotation.rest.At
import net.csdn.common.collections.WowCollections
import net.csdn.modules.http.RestRequest.Method._
import net.csdn.modules.http.{ApplicationController, ViewType}
import net.sf.json.JSONObject
import streaming.core.Dispatcher
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime, SparkStreamingRuntime}

import scala.collection.JavaConversions._

/**
 * 4/30/16 WilliamZhu(allwefantasy@gmail.com)
 */
class RestController extends ApplicationController {
  @At(path = Array("/runtime/spark/streaming/stop"), types = Array(GET))
  def stopRuntime = {
    runtime(PlatformManager.SPAKR_STREAMING).destroyRuntime(true)
    render(200, "ok")
  }

  @At(path = Array("/runtime/spark/sql"), types = Array(POST))
  def sql = {
    if (!runtime(PlatformManager.SPARK).isInstanceOf[SparkRuntime]) render(400, "only support spark application")
    val sparkRuntime = runtime(PlatformManager.SPARK).asInstanceOf[SparkRuntime]
    sparkRuntime.operator.createTable(param("resource"), param("tableName"), params().filter(f => f._1.startsWith("es.")).toMap)
    val result = sparkRuntime.operator.runSQL(param("sql")).mkString("\n")
    render(result, ViewType.string)
  }

  @At(path = Array("/index"), types = Array(GET))
  def index = {
    renderHtml(200, "/rest/index.vm", WowCollections.map())
  }

  @At(path = Array("/runtime/spark/streaming/job/add"), types = Array(POST))
  def addJob = {
    if (!runtime(PlatformManager.SPAKR_STREAMING).isInstanceOf[SparkStreamingRuntime]) render(400, "only support spark streaming application")
    val _runtime = runtime(PlatformManager.SPAKR_STREAMING).asInstanceOf[SparkStreamingRuntime]
    val waitCounter = new AtomicInteger(0)
    while (!_runtime.streamingRuntimeInfo.sparkStreamingOperator.isStreamingCanStop()
      && waitCounter.get() < paramAsInt("waitRound", 1000)) {
      Thread.sleep(50)
      waitCounter.incrementAndGet()
    }
    dispatcher.createStrategy(param("name"), JSONObject.fromObject(request.contentAsString()))
    if (_runtime.streamingRuntimeInfo.sparkStreamingOperator.isStreamingCanStop()) {
      _runtime.destroyRuntime(false)
      new Thread(new Runnable {
        override def run(): Unit = {
          platformManager.run(null, true)
        }

      }).start()

      render(200, "ok")
    } else {
      render(400, "timeout")
    }

  }

  def platformManager = PlatformManager.getOrCreate

  def dispatcher = {
    platformManager.findDispatcher(Dispatcher.contextParams(""))
  }

  def runtime(name: String) = PlatformManager.getRuntime(name, new java.util.HashMap[Any, Any]())
}
