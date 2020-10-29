package streaming.rest

import net.csdn.annotation.rest.At
import net.csdn.common.collections.WowCollections
import net.csdn.modules.http.ApplicationController
import net.csdn.modules.http.RestRequest.Method.{GET, POST}
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import tech.mlsql.common.utils.serder.json.JSONTool

/**
 * 29/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class HealthController extends ApplicationController {
  /**
   * HTTP/1.1 200 OK
   * {
   * "status": "UP",
   * "components": {
   * "livenessProbe": {
   * "status": "UP"
   * }
   * }
   * }
   */
  @At(path = Array("/health/liveness"), types = Array(GET, POST))
  def liveness = {
    val r = runtime
    if(r!=null){
      val sr = r.asInstanceOf[SparkRuntime]
      if(sr.sparkSession.sparkContext.isStopped){
        render(500, JSONTool.toJsonStr(Map(
          "status" -> "DOWN",
          "components" -> Map(
            "livenessProbe" -> Map("status" -> "DOWN")
          )
        )))
      }
    }

    render(200, JSONTool.toJsonStr(Map(
      "status" -> "UP",
      "components" -> Map(
        "livenessProbe" -> Map("status" -> "UP")
      )
    )))
  }

  /**
   * HTTP/1.1 503 SERVICE UNAVAILABLE
   * {
   * "status": "OUT_OF_SERVICE",
   * "components": {
   * "readinessProbe": {
   * "status": "OUT_OF_SERVICE"
   * }
   * }
   * }
   */
  @At(path = Array("/health/readiness"), types = Array(GET, POST))
  def readiness = {
    if(PlatformManager.RUNTIME_IS_READY.get()){
      render(200, JSONTool.toJsonStr(Map(
        "status" -> "IN_SERVICE",
        "components" -> Map(
          "readinessProbe" -> Map("status" -> "IN_SERVICE")
        )
      )))
    }
    render(503, JSONTool.toJsonStr(Map(
      "status" -> "OUT_OF_SERVICE",
      "components" -> Map(
        "readinessProbe" -> Map("status" -> "OUT_OF_SERVICE")
      )
    )))

  }
  def runtime = PlatformManager.getRuntime
}
