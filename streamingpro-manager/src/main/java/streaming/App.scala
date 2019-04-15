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

package streaming

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Application
import org.apache.velocity.app.Velocity
import streaming.common.ParamsUtil
import streaming.db.ManagerConfiguration
import streaming.service.MonitorScheduler

/**
  * Created by allwefantasy on 12/7/2017.
  */
object App {
  def main(args: Array[String]): Unit = {
    ManagerConfiguration.config = new ParamsUtil(args)
    require(ManagerConfiguration.config.hasParam("yarnUrl"), "-yarnUrl is required")
    ServiceFramwork.scanService.setLoader(classOf[App])
    ServiceFramwork.registerStartWithSystemServices(classOf[MonitorScheduler])
    Application.main(Array())
  }
}

object StreamingManagerApp {
  def main(args: Array[String]): Unit = {
    App.main(args)
  }
}
