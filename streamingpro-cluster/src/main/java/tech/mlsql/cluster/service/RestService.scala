package tech.mlsql.cluster.service

import net.csdn.ServiceFramwork
import net.csdn.common.settings.Settings
import net.csdn.modules.threadpool.DefaultThreadPoolService
import net.csdn.modules.transport.{DefaultHttpTransportService, HttpTransportService}

/**
  * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
  */
object RestService {
  private final val settings: Settings = ServiceFramwork.injector.getInstance(classOf[Settings])
  private final val transportService: HttpTransportService = new DefaultHttpTransportService(new DefaultThreadPoolService(settings), settings)

  def client(url: String): BackendService = BackendRestClient.buildClient[BackendService](url, transportService)
}
