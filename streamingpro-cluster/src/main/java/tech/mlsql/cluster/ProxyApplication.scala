package tech.mlsql.cluster

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Application


/**
  * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
  */

object ProxyApplication {
  def main(args: Array[String]): Unit = {
    ServiceFramwork.scanService.setLoader(classOf[ProxyApplication])
    Application.main(args)
  }
}


class ProxyApplication


