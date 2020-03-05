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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.{MLSQLConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.cluster.PSDriverEndpoint
import org.apache.spark.security.CryptoStreamUtils

/**
 * Created by allwefantasy on 30/1/2018.
 */
class PSDriverBackend(sc: SparkContext) extends Logging {

  val conf = sc.conf
  var psDriverRpcEndpointRef: RpcEndpointRef = null

  def createRpcEnv = {
    val isDriver = sc.env.executorId == SparkContext.DRIVER_IDENTIFIER
    val bindAddress = sc.conf.get(DRIVER_BIND_ADDRESS)
    val advertiseAddress = sc.conf.get(DRIVER_HOST_ADDRESS)
    val port = sc.conf.getOption(MLSQLConf.MLSQL_CLUSTER_PS_DRIVER_PORT.key).getOrElse("7777").toInt
    val ioEncryptionKey = if (sc.conf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(sc.conf))
    } else {
      None
    }
    logInfo(s"setup ps driver rpc env: ${bindAddress}:${port} clientMode=${!isDriver}")
    val env = new AtomicReference[RpcEnv]()
    try {
      env.set(RpcEnv.create("PSDriverEndpoint", bindAddress, port, sc.conf,
        sc.env.securityManager, clientMode = !isDriver))
    } catch {
      case e: Exception =>
        logInfo("fail to create rpcenv", e)
    }
    if (env.get() == null) {
      logError(s"fail to create rpcenv finally")
    }
    env.get()
  }

  def start() = {
    val env = createRpcEnv
    val pSDriverBackend = new PSDriverEndpoint(sc, env)
    psDriverRpcEndpointRef = env.setupEndpoint("ps-driver-endpoint", pSDriverBackend)
  }

}


