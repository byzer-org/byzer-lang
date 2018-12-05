package tech.mlsql.cluster

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Application
import streaming.common.ParamsUtil


/**
  * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
  */

object ProxyApplication {
  def main(args: Array[String]): Unit = {
    val params = new ParamsUtil(args)
    val applicationYamlName = params.getParam("config", "application.yml")
    ServiceFramwork.applicaionYamlName(applicationYamlName)
    ServiceFramwork.scanService.setLoader(classOf[ProxyApplication])
    Application.main(args)
  }
}


class ProxyApplication


