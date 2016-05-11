package streaming.rest

import java.util.concurrent.atomic.AtomicInteger

import net.csdn.annotation.rest.At
import net.csdn.common.collections.WowCollections
import net.csdn.modules.http.{ViewType, ApplicationController}
import net.csdn.modules.http.RestRequest.Method._
import net.sf.json.JSONObject
import serviceframework.dispatcher.StrategyDispatcher
import streaming.core.strategy.platform.{PlatformManager, SparkStreamingRuntime}

/**
 * 4/30/16 WilliamZhu(allwefantasy@gmail.com)
 */
class RestController extends ApplicationController {
  @At(path = Array("/runtime/stop"), types = Array(GET))
  def stopRuntime = {
    runtime.destroyRuntime(true)
    render(200, "ok")
  }

  @At(path = Array("/index"), types = Array(GET))
  def index = {
    renderHtml(200, "/rest/index.vm",WowCollections.map())
  }

  @At(path = Array("/job/add"), types = Array(POST))
  def addJob = {
    if (!runtime.isInstanceOf[SparkStreamingRuntime]) render(400, "only support Spark Streaming")
    val _runtime = runtime.asInstanceOf[SparkStreamingRuntime]
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

  def dispatcher = StrategyDispatcher.getOrCreate()

  def runtime = PlatformManager.getRuntime(null, new java.util.HashMap[Any, Any]())
}
