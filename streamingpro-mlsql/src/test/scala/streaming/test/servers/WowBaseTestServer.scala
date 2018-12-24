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

package streaming.test.servers

import java.util.UUID

import streaming.common.shell.ShellCommand
import streaming.log.Logging

trait WowBaseTestServer extends Logging {
  val COMPOSE_FILE_DIR = "/tmp/__mlsql__/test/compose_file/"
  val pjDir = COMPOSE_FILE_DIR + UUID.randomUUID().toString

  def composeYaml: String

  def checkEngineIsOk: Boolean = {
    val (status, _, _) = ShellCommand.execWithExitValue("docker -v", -1)
    if (status != 0) return false
    val (status2, _, _) = ShellCommand.execWithExitValue("docker-compose -v", -1)
    if (status2 != 0) return false
    return true;
  }

  def waitToServiceReady: Boolean

  protected def readyCheck(serviceName: String, command: String, inDocker: Boolean = true): Boolean = {
    //echo stat | nc 127.0.0.1 2182
    val wrapCommand = if (inDocker)
      s"""
         |cd $pjDir;docker-compose -p ${projectName} exec -T ${serviceName} bash -c "${command}"
       """.stripMargin else {
      command
    }
    var counter = 60
    var success = false
    while (!success && counter > 0) {
      val (status, out, error) = ShellCommand.execWithExitValue(wrapCommand, -1)
      if (status == 0) {
        success = true
      }
      Thread.sleep(1000)
      logInfo(s"execute command:${wrapCommand}")
      logInfo(s"checking count: ${counter} status:${status} out:${out} error:${error}")
      counter -= 1
    }
    success
  }

  def exec(serviceName: String, command: String) = {
    val wrapCommand =
      s"""
         |cd $pjDir;docker-compose -p ${projectName} exec -T ${serviceName} bash -c "${command}"
      """.stripMargin
    val (status, out, error) = ShellCommand.execWithExitValue(wrapCommand, -1)
    logInfo(s"command=[${wrapCommand}] status=${status} out=[${out}] err=[${error}]")
    require(status == 0, "execute fail")
    (out, error)
  }

  def projectName = pjDir.split("/").last.replaceAll("-", "")

  def startServer = {

    ShellCommand.execCmd(s"mkdir -p ${pjDir}")
    ShellCommand.writeToFile(pjDir + "/docker-compose.yml", composeYaml)
    val dockerComposeCommand = s"cd ${pjDir};docker-compose -p ${projectName}  up -d"
    val (status, out, err) = ShellCommand.execWithExitValue(dockerComposeCommand, -1)
    logInfo(s"command=[${dockerComposeCommand}] status=${status} out=[${out}] err=[${err}]")
    require(status == 0, "Fail to start server")
    waitToServiceReady
  }

  def stopServer = {
    val dockerComposeCommand = s"cd ${pjDir};docker-compose -p ${projectName} down"
    val (status, out, err) = ShellCommand.execWithExitValue(dockerComposeCommand, -1)
    logInfo(s"command=[${dockerComposeCommand}] status=${status} out=[${out}] err=[${err}]")
    require(status == 0, "Fail to start server")
    ShellCommand.execCmd(s"rm -rf ${pjDir}")
    //ShellCommand.execWithExitValue("docker network prune --force")
  }
}
