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
