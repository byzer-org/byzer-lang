package org.apache.spark.ps.cluster

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.cluster.PSDriverEndpoint
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.{MLSQLConf, SparkContext}

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
    var createSucess = false

    val env = new AtomicReference[RpcEnv]()
    try {
      env.set(RpcEnv.create("PSDriverEndpoint", bindAddress, port, sc.conf,
        sc.env.securityManager, clientMode = !isDriver))
      createSucess = true
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


