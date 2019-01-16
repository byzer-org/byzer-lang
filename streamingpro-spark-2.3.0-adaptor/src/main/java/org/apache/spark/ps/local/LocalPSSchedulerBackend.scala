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

package org.apache.spark.ps.local

import java.io.File
import java.net.URL

import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.ps.cluster.Message
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.StopExecutor
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import streaming.common.HDFSOperator
import streaming.dsl.mmlib.algs.python.BasicCondaEnvManager


private case class TensorFlowModelClean(modelPath: String)


/**
  * Calls to [[LocalPSSchedulerBackend]] are all serialized through LocalEndpoint. Using an
  * RpcEndpoint makes the calls on [[LocalPSSchedulerBackend]] asynchronous, which is necessary
  * to prevent deadlock between [[LocalPSSchedulerBackend]] and the [[TaskSchedulerImpl]].
  */
class LocalPSEndpoint(override val rpcEnv: RpcEnv,
                      userClassPath: Seq[URL]

                     )
  extends ThreadSafeRpcEndpoint with Logging {

  val localExecutorId = SparkContext.DRIVER_IDENTIFIER
  val localExecutorHostname = "localhost"

  override def receive: PartialFunction[Any, Unit] = {
    case Message.Pong(id) =>
      logInfo(s"received message ${Message.Pong} from executor ${id}!")
    case _ =>

  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case Message.CopyModelToLocal(modelPath, destPath) => {
      logInfo(s"copying model: ${modelPath} -> ${destPath}")
      HDFSOperator.copyToLocalFile(destPath, modelPath, true)
      context.reply(true)
    }
    case Message.CreateOrRemovePythonCondaEnv(condaYamlFile, options, command) => {
      val condaEnvManager = new BasicCondaEnvManager(options)
      command match {
        case Message.AddEnvCommand =>
          condaEnvManager.getOrCreateCondaEnv(Option(condaYamlFile))
        case Message.RemoveEnvCommand =>
          condaEnvManager.removeEnv(Option(condaYamlFile))
      }
      context.reply(1)
    }
    case Message.Ping =>
      logInfo(s"received message ${Message.Ping}")
      val response = Message.Pong("localhost")
      self.send(response)
      context.reply(response)
  }
}


class LocalPSSchedulerBackend(sparkContext: SparkContext)
  extends Logging {

  private val appId = "local-ps-" + System.currentTimeMillis
  var localEndpoint: RpcEndpointRef = null
  private val userClassPath = getUserClasspath(sparkContext.getConf)
  private val launcherBackend = new LauncherBackend() {
    override def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)

    protected def conf: SparkConf = sparkContext.conf
  }

  /**
    * Returns a list of URLs representing the user classpath.
    *
    * @param conf Spark configuration.
    */
  def getUserClasspath(conf: SparkConf): Seq[URL] = {
    val userClassPathStr = conf.getOption("spark.executor.extraClassPath")
    userClassPathStr.map(_.split(File.pathSeparator)).toSeq.flatten.map(new File(_).toURI.toURL)
  }

  launcherBackend.connect()

  def start() {
    val rpcEnv = SparkEnv.get.rpcEnv
    val executorEndpoint = new LocalPSEndpoint(rpcEnv, userClassPath)
    localEndpoint = rpcEnv.setupEndpoint("PSLocalSchedulerBackend", executorEndpoint)
    launcherBackend.setAppId(appId)
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
    LocalExecutorBackend.executorBackend = Some(this)
  }

  def stop() {
    stop(SparkAppHandle.State.FINISHED)
  }

  private def stop(finalState: SparkAppHandle.State): Unit = {
    localEndpoint.ask(StopExecutor)
    try {
      launcherBackend.setState(finalState)
    } finally {
      launcherBackend.close()
    }
  }

}

object LocalExecutorBackend {
  var executorBackend: Option[LocalPSSchedulerBackend] = None
}
