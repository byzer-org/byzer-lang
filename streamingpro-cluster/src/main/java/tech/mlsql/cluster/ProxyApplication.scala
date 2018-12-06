package tech.mlsql.cluster

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Application
import streaming.common.ParamsUtil
import tech.mlsql.cluster.service.elastic_resource.AllocateService


/**
  * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
  */

object ProxyApplication {
  var commandConfig: ProxyApplication = null

  def main(args: Array[String]): Unit = {
    val params = new ParamsUtil(args)
    commandConfig = new ProxyApplication(params)
    val applicationYamlName = params.getParam("config", "application.yml")
    ServiceFramwork.applicaionYamlName(applicationYamlName)
    ServiceFramwork.scanService.setLoader(classOf[ProxyApplication])
    AllocateService.run
    Application.main(args)
  }
}


class ProxyApplication(params: ParamsUtil) {
  def allocateCheckInterval = {
    params.getIntParam("allocateCheckInterval", 10)
  }
}


