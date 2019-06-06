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

package streaming.core

import streaming.common.ParamsUtil
import streaming.core.strategy.platform.PlatformManager


object StreamingApp {

  def main(args: Array[String]): Unit = {
    val params = new ParamsUtil(args)
    configureLogProperty(params)
    require(params.hasParam("streaming.name"), "Application name should be set")
    PlatformManager.getOrCreate.run(params)
  }

  def configureLogProperty(params: ParamsUtil) = {
    if (System.getProperty("REALTIME_LOG_HOME") == null) {
      System.setProperty("REALTIME_LOG_HOME", params.getParam("REALTIME_LOG_HOME",
        "/tmp/__mlsql__/logs"))
    }
  }
}

class StreamingApp
