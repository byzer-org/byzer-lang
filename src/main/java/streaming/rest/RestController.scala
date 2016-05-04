package streaming.rest

import net.csdn.annotation.rest.At
import net.csdn.modules.http.ApplicationController
import net.csdn.modules.http.RestRequest.Method._
import net.sf.json.JSONObject
import serviceframework.dispatcher.StrategyDispatcher
import streaming.core.strategy.platform.PlatformManager

/**
 * 4/30/16 WilliamZhu(allwefantasy@gmail.com)
 */
class RestController extends ApplicationController {
  @At(path = Array("/runtime/stop"), types = Array(GET))
  def stopRuntime = {
    runtime.destroyRuntime(true)
    render(200, "ok")
  }

  @At(path = Array("/job/add"), types = Array(POST))
  def addJob = {
    dispatcher.createStrategy(param("name"), JSONObject.fromObject(request.contentAsString()))
    runtime.destroyRuntime(false)
    platformManager.run(null, true)
    render(200, "ok")
  }

  def platformManager = PlatformManager.getOrCreate

  def dispatcher = StrategyDispatcher.getOrCreate()

  def runtime = PlatformManager.getRuntime(null, new java.util.HashMap[Any, Any]())
}
