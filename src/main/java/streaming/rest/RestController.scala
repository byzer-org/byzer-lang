package streaming.rest

import net.csdn.annotation.rest.At
import net.csdn.modules.http.ApplicationController
import net.csdn.modules.http.RestRequest.Method._
import streaming.core.strategy.platform.PlatformManager

/**
 * 4/30/16 WilliamZhu(allwefantasy@gmail.com)
 */
class RestController extends ApplicationController {
  @At(path = Array("/runtime/stop"), types = Array(GET))
  def stopRuntime = {
    PlatformManager.getRuntime(null, new java.util.HashMap[Any, Any]()).destroyRuntime
    render(200, "ok")
  }
}
