package tech.mlsql.cluster.service

import java.lang.reflect.Proxy

import com.alibaba.dubbo.rpc.protocol.rest.RestClientProxy
import net.csdn.modules.transport.HttpTransportService

/**
  * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
  */
object BackendRestClient {

  def buildClient[T](url: String, transportService: HttpTransportService)(implicit manifest: Manifest[T]): T = {
    val restClientProxy = new RestClientProxy(transportService)
    if (url.startsWith("http:")) {
      restClientProxy.target(url)
    } else {
      restClientProxy.target("http://" + url + "/")
    }
    val clazz = manifest.runtimeClass
    Proxy.newProxyInstance(clazz.getClassLoader, Array(clazz), restClientProxy).asInstanceOf[T]
  }
}
