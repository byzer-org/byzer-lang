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

import java.net.URL
import java.util.Properties

import com.codahale.metrics.MetricRegistry
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.metrics.sink.Sink
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.{SecurityManager, SparkContext, SparkEnv}

import scala.collection.mutable

/**
  * Created by allwefantasy on 2/2/2018.
  */
class PSServiceSink(val property: Properties, val registry: MetricRegistry,
                    securityMgr: SecurityManager) extends Sink with Logging {
  def env = SparkEnv.get

  var psDriverUrl: String = null
  var psExecutorId: String = null
  var hostname: String = null
  var cores: Int = 0
  var appId: String = null
  var psDriverPort = 7777
  var psDriverHost: String = null
  var workerUrl: Option[String] = None
  val userClassPath = new mutable.ListBuffer[URL]()

  def parseArgs = {
    //val runtimeMxBean = ManagementFactory.getRuntimeMXBean();
    //var argv = runtimeMxBean.getInputArguments.toList
    var argv = System.getProperty("sun.java.command").split("\\s+").toList

    var count = 0
    var first = 0
    argv.foreach { f =>
      if (f.startsWith("--") && first == 0) {
        first = count
      }
      count += 1
    }
    argv = argv.drop(first)

    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          psDriverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          psExecutorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
      }
    }
    if (psDriverUrl.contains("@")) {
      psDriverUrl = psDriverUrl.split("@").last
    }
    val Array(host, port) = psDriverUrl.split(":")
    psDriverHost = host
    // this port have been set in SparkRuntime dynamically
    psDriverPort = env.conf.getInt("spark.ps.cluster.driver.port", 0)
    if (psDriverPort == 0) {
      throw new RuntimeException("Executor psDriverPort can not get spark.ps.cluster.driver.port")
    }
    psDriverUrl = "spark://ps-driver-endpoint@" + psDriverHost + ":" + psDriverPort
  }

  def createRpcEnv = {
    val isDriver = env.executorId == SparkContext.DRIVER_IDENTIFIER
    val bindAddress = hostname
    val advertiseAddress = ""
    val port = env.conf.getOption("spark.ps.executor.port").getOrElse("0").toInt
    val ioEncryptionKey = if (env.conf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(env.conf))
    } else {
      None
    }
    //logInfo(s"setup ps driver rpc env: ${bindAddress}:${port} clientMode=${!isDriver}")
    RpcEnv.create("PSExecutorBackend", bindAddress, port, env.conf,
      env.securityManager, clientMode = !isDriver)
  }

  override def start(): Unit = {

    new Thread(new Runnable {
      override def run(): Unit = {
        logInfo(s"delay PSExecutorBackend 3s")
        var counter = 30
        while (SparkEnv.get == null && counter > 0) {
          Thread.sleep(1000)
          counter -= 1
        }

        if (SparkEnv.get == null) {
          throw new RuntimeException("cannot get SparkEnv in PSServiceSink")
        }
        logInfo(s"start PSExecutor;env:${env}")
        if (env.executorId != SparkContext.DRIVER_IDENTIFIER) {
          parseArgs
          val rpcEnv = createRpcEnv
          val pSExecutorBackend = new PSExecutorBackend(env, rpcEnv, psDriverUrl, psExecutorId, hostname, cores)
          PSExecutorBackend.executorBackend = Some(pSExecutorBackend)
          rpcEnv.setupEndpoint("ps-executor-endpoint", pSExecutorBackend)
        }
      }
    }).start()

  }

  override def stop(): Unit = {

  }

  override def report(): Unit = {

  }
}


