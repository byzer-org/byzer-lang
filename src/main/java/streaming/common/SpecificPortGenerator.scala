package streaming.common

import java.util

import net.csdn.modules.http.PortGenerator
import streaming.common.ParamsHelper._
import streaming.core.strategy.platform.PlatformManager

/**
 * 5/26/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SpecificPortGenerator extends PortGenerator {
  override def getPort: Int = {
    val runtime = PlatformManager.getRuntime
    runtime.params.paramAsInt("streaming.driver.port", 9003)
  }
}
