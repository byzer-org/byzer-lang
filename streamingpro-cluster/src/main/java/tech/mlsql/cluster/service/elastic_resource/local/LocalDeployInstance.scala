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

package tech.mlsql.cluster.service.elastic_resource.local

import net.sf.json.JSONObject
import tech.mlsql.cluster.model.{Backend, EcsResourcePool}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.shell.ShellCommand

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
  */
object LocalDeployInstance extends Logging {

  def _deploy(ecs: EcsResourcePool, dryRun: Boolean = false) = {
    var success = true
    try {
      logInfo(
        s"""
           |${ecs.getLoginUser} login in ${ecs.getIp} and execute command with user ${ecs.getExecuteUser}.
           |The content of script:
           |${startScript(ecs)}
         """.stripMargin)
      ShellCommand.sshExec(ecs.getKeyPath, ecs.getIp, ecs.getLoginUser, startScript(ecs), ecs.getExecuteUser, dryRun)
    } catch {
      case e: Exception =>
        logError("fail to deploy new instance", e)
        success = false
    }
    success
  }

  def deploy(id: Int): Boolean = {
    val ecs = EcsResourcePool.findById(id)
    val success = _deploy(ecs)
    if (success) {
      logInfo(s"Command execute successful, remove ${ecs.getIp} with name ${ecs.getName} from EcsResourcePool to Backend table.")
      Backend.newOne(Map(
        "url" -> (ecs.getIp + ":" + getPort(ecs)),
        "tag" -> ecs.getTag,
        "name" -> ecs.getName,
        "ecsResourcePoolId" -> (ecs.id() + "")
      ).asJava, true).save()
      ecs.delete()
    }
    return success

  }

  def _unDeploy(backend: Backend, ecs: EcsResourcePool, dryRun: Boolean = false) = {
    var success = true
    try {
      ShellCommand.sshExec(ecs.getKeyPath, ecs.getIp, ecs.getLoginUser, shutdownScript(ecs), ecs.getExecuteUser, dryRun)
    } catch {
      case e: Exception =>
        logError("fail to remove instance", e)
        success = false
    }
    success
  }

  def unDeploy(id: Int): Boolean = {
    val backend = Backend.findById(id)
    if (backend.getEcsResourcePoolId == -1) {
      logError(s"cannot find ${backend.getName} in resource pool")
      return false
    }
    val ecs = EcsResourcePool.findById(backend.getEcsResourcePoolId)
    val success = _unDeploy(backend, ecs, false)
    if (success) {
      ecs.setInUse(EcsResourcePool.NOT_IN_USE)
      ecs.save()
      backend.delete()
    }
    return success
  }


  def getPort(ecs: EcsResourcePool) = {
    val port = JSONObject.fromObject(ecs.getMlsqlConfig).asScala.filter { k =>
      k._1.toString == "streaming.driver.port"
    }.map(f => f._2.toString).headOption
    require(port.isDefined, "MLSQL port should be configured.")
    port.get
  }

  private def shutdownScript(ecs: EcsResourcePool) = {
    s"""
       |#/bin/bash
       |process_id=$$(ps -ef |grep streamingpro-mlsql|grep -v "grep"|awk '{print $$2}')
       |kill -9 $${process_id}
     """.stripMargin
  }

  private def startScript(ecs: EcsResourcePool) = {
    val sparkConfigBuffer = ArrayBuffer[String]()
    val streamingConfigBuffer = ArrayBuffer[String]()
    JSONObject.fromObject(ecs.getMlsqlConfig).asScala.map { k =>
      val key = k._1.toString
      val value = k._2.toString
      if (key.startsWith("streaming.")) {
        streamingConfigBuffer += s"-${key} $value"
      } else {
        sparkConfigBuffer += s"--${key} $value"
      }
    }
    val configBuffer = sparkConfigBuffer ++ ArrayBuffer("${MLSQL_HOME}/libs/${MAIN_JAR}") ++ streamingConfigBuffer
    val startUpConfig = configBuffer.mkString(" \\\n")

    s"""
       |#/bin/bash
       |export MLSQL_HOME=${ecs.getMlsqlHome}
       |JARS=$$(echo $${MLSQL_HOME}/libs/*.jar | tr ' ' ',')
       |MAIN_JAR=$$(ls $${MLSQL_HOME}/libs|grep 'streamingpro-mlsql')
       |
       |export SPARK_HOME=${ecs.getSparkHome}
       |cd $$SPARK_HOME
       |nohup ./bin/spark-submit --class streaming.core.StreamingApp \\
       |        ${startUpConfig} > wow.log &
     """.stripMargin
  }

}
