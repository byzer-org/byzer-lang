package org.apache.spark.ps.cluster

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkContext
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
    var port = sc.conf.getOption("spark.ps.driver.port").getOrElse("7777").toInt
    val ioEncryptionKey = if (sc.conf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(sc.conf))
    } else {
      None
    }
    logInfo(s"setup ps driver rpc env: ${bindAddress}:${port} clientMode=${!isDriver}")
    var createSucess = false
    var count = 0
    val env = new AtomicReference[RpcEnv]()
    while (!createSucess && count < 10) {
      try {
        env.set(RpcEnv.create("PSDriverEndpoint", bindAddress, port, sc.conf,
          sc.env.securityManager, clientMode = !isDriver))
        createSucess = true
      } catch {
        case e: Exception =>
          logInfo("fail to create rpcenv", e)
          count += 1
          port += 1
      }
    }
    if (env.get() == null) {
      logError(s"fail to create rpcenv finally with attemp ${count} ")
    }
    env.get()
  }

  def start() = {
    val env = createRpcEnv
    val pSDriverBackend = new PSDriverEndpoint(sc, env)
    psDriverRpcEndpointRef = env.setupEndpoint("ps-driver-endpoint", pSDriverBackend)
  }

}


