package streaming.core

import java.util.{List => JList, Map => JMap}

import streaming.common.ParamsUtil
import streaming.core.strategy.platform.PlatformManager


object StreamingApp {

  def main(args: Array[String]): Unit = {
    val params = new ParamsUtil(args)
    require(params.hasParam("streaming.name"), "Application name should be set")
    PlatformManager.getOrCreate.run(params)
  }

}

class StreamingApp
