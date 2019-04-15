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

package org.apache.spark.ps.cluster

import java.util.Locale

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.util.ThreadUtils
import streaming.common.HDFSOperator
import streaming.dsl.mmlib.algs.python.BasicCondaEnvManager

import scala.util.{Failure, Success}


/**
  * Created by allwefantasy on 30/1/2018.
  */
class PSExecutorBackend(env: SparkEnv, override val rpcEnv: RpcEnv, psDriverUrl: String, psExecutorId: String, hostname: String, cores: Int) extends ThreadSafeRpcEndpoint with Logging {

  override def onStart(): Unit = {

    logInfo("Connecting to driver: " + psDriverUrl)
    rpcEnv.asyncSetupEndpointRefByURI(psDriverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      val driver = Some(ref)
      ref.ask[Boolean](Message.RegisterPSExecutor(psExecutorId, self, hostname, cores, extractLogUrls))
    }(ThreadUtils.sameThread).onComplete {
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      case Success(msg) =>
        logInfo(s"${psExecutorId}@${hostname} register with driver: $psDriverUrl success")
      // Always receive `true`. Just ignore it
      case Failure(e) =>
        logError(s"Cannot register with driver: $psDriverUrl", e)
    }(ThreadUtils.sameThread)
  }

  def extractLogUrls: Map[String, String] = {
    val prefix = "SPARK_LOG_URL_"
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toLowerCase(Locale.ROOT), e._2))
  }

  override def receive: PartialFunction[Any, Unit] = {
    case Message.RegisteredExecutor =>
    case Message.RegisterExecutorFailed =>

  }


  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case Message.CopyModelToLocal(modelPath, destPath) => {
      logInfo(s"copying model: ${modelPath} -> ${destPath}")
      HDFSOperator.copyToLocalFile(destPath, modelPath, true)
      context.reply(true)
    }

    case Message.CreateOrRemovePythonCondaEnv(condaYamlFile, options, command) => {
      val success = try {
        val condaEnvManager = new BasicCondaEnvManager(options)
        command match {
          case Message.AddEnvCommand =>
            condaEnvManager.getOrCreateCondaEnv(Option(condaYamlFile))
          case Message.RemoveEnvCommand =>
            condaEnvManager.removeEnv(Option(condaYamlFile))
        }
        true
      } catch {
        case e: Exception =>
          logError("Create PythonEnv fail", e)
          false
      }

      context.reply(success)
    }
    case Message.Ping =>
      logInfo(s"received message ${Message.Ping}")
      context.reply(Message.Pong(psExecutorId))
  }
}

object PSExecutorBackend {
  var executorBackend: Option[PSExecutorBackend] = None
}


