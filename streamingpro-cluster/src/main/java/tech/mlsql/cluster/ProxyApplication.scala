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

package tech.mlsql.cluster

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Application
import tech.mlsql.cluster.commands.Command
import tech.mlsql.cluster.service.elastic_resource.AllocateService
import tech.mlsql.common.utils.shell.command.ParamsUtil


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
    if (params.hasParam("command")) {
      ServiceFramwork.enableNoThreadJoin()
      ServiceFramwork.disableHTTP()
      Application.main(args)
      Command.deploy
    } else {
      AllocateService.run
      Application.main(args)
    }

  }
}


class ProxyApplication(params: ParamsUtil) {
  def allocateCheckInterval = {
    params.getIntParam("allocateCheckInterval", 10)
  }
}


