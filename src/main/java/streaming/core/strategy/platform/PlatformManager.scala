package streaming.core.strategy.platform

import java.util.{List => JList, Map => JMap}

/**
 * 4/27/16 WilliamZhu(allwefantasy@gmail.com)
 */
object PlatformManager {

  def getRuntime(name: String, params: JMap[Any, Any]) = {
    SparkStreamingRuntime.getOrCreate(params)
  }
}
