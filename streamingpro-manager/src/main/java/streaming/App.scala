package streaming

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Application
import org.apache.velocity.app.Velocity
import streaming.common.ParamsUtil
import streaming.db.ManagerConfiguration
import streaming.service.Scheduler

/**
  * Created by allwefantasy on 12/7/2017.
  */
object App {
  def main(args: Array[String]): Unit = {
    Velocity.addProperty("velocimacro.permissions.allow.inline.to.replace.global",true)
    Velocity.addProperty("velocimacro.library.autoreload",true)
    Velocity.addProperty("file.resource.loader.cache",false)
    Velocity.addProperty("file.resource.loader.modificationCheckInterval",2)
    ManagerConfiguration.config = new ParamsUtil(args)
    require(ManagerConfiguration.config.hasParam("yarnUrl"), "-yarnUrl is required")
    ServiceFramwork.scanService.setLoader(classOf[App])
    ServiceFramwork.registerStartWithSystemServices(classOf[Scheduler])
    Application.main(Array())
  }
}
