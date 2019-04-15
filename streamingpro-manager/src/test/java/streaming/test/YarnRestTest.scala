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

package streaming.test

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files
import org.scalatest.{FlatSpec, Matchers}
import streaming.common.ParamsUtil
import streaming.db.{DB, ManagerConfiguration, TSparkApplication}
import streaming.service.{MonitorScheduler, YarnRestService}
import streaming.shell.ShellCommand

/**
  * Created by allwefantasy on 17/7/2017.
  */
class YarnRestTest extends FlatSpec with Matchers {

  ManagerConfiguration.config = new ParamsUtil(Array(
    "-jdbcPath", "classpath:///jdbc.properties",
    "-yarnUrl", "",
    "-afterLogCheckTimeout", "5",
    "-enableScheduler", "false"
  ))

  DB

  "after shell" should "execute timeout" in {


    val taskId = "_abc"
    val logPath = s"/tmp/mammuthus/${taskId}/stdout"

    val content = "INFO  17-07 10:57:16,868 - Application report for application_1499754907574_36282 (state: ACCEPTED)\nINFO  17-07 10:57:17,869 - Application report for application_1499754907574_36282 (state: ACCEPTED)\nINFO  17-07 10:57:18,871 - Application report for application_1499754907574_36282 (state: ACCEPTED)\nINFO  17-07 10:57:18,904 - ApplicationMaster registered as NettyRpcEndpointRef(null)"
    ShellCommand.exec(s"mkdir -p /tmp/mammuthus/${taskId}/")
    Files.write(content, new File(logPath), Charset.forName("utf-8"))

    val app = TSparkApplication.save("application_1499754907574_36282", "master", "spark-submit...", "", "echo yes")
    val task = streaming.service.Task(taskId, "", app.id)

    MonitorScheduler.sparkSubmitTaskMap.put(task, System.currentTimeMillis())


    MonitorScheduler.checkSubmitAppStateTask(app.id, task)
    assume(!MonitorScheduler.sparkSubmitTaskMap.contains(task))
    Thread.sleep(10 * 1000)
    val logs = TSparkApplication.queryLog(app.id)

    assume(logs.size > 0)
    assume(logs(0).failReason == "execute after shell timeout: 60 seconds")


  }

  "after shell" should "exeute success" in {

    val taskId = "_abc"
    val logPath = s"/tmp/mammuthus/${taskId}/stdout"

    var content = "INFO  17-07 10:57:16,868 - Application report for application_1499754907574_36282 (state: ACCEPTED)\nINFO  17-07 10:57:17,869 - Application report for application_1499754907574_36282 (state: ACCEPTED)\nINFO  17-07 10:57:18,871 - Application report for application_1499754907574_36282 (state: ACCEPTED)\nINFO  17-07 10:57:18,904 - ApplicationMaster registered as NettyRpcEndpointRef(null)"
    ShellCommand.exec(s"mkdir -p /tmp/mammuthus/${taskId}/")
    Files.write(content, new File(logPath), Charset.forName("utf-8"))

    val app = TSparkApplication.save("application_1499754907574_36282", "master", "spark-submit...", "", "echo yes")
    val task = streaming.service.Task(taskId, "", app.id)

    MonitorScheduler.sparkSubmitTaskMap.put(task, System.currentTimeMillis())



    MonitorScheduler.checkSubmitAppStateTask(app.id, task)
    assume(!MonitorScheduler.sparkSubmitTaskMap.contains(task))

    content = "INFO  17-07 10:57:18,913 - Add WebUI Filter. org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter, Map(PROXY_HOSTS -> master1.uc.host.dxy, PROXY_URI_BASES -> http://master1.uc.host.dxy:8088/proxy/application_1499754907574_36282), /proxy/application_1499754907574_36282\nINFO  17-07 10:57:18,914 - Adding filter: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter\nINFO  17-07 10:57:19,873 - Application report for application_1499754907574_36282 (state: RUNNING)"
    Files.write(content, new File(logPath), Charset.forName("utf-8"))
    Thread.sleep(2000)
    val logs = TSparkApplication.queryLog(app.id)
    assume(logs.size == 0)
  }
}
