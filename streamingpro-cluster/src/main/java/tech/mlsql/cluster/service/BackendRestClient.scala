/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
