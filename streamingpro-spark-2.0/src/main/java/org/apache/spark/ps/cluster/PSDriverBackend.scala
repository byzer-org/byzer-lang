package org.apache.spark.ps.cluster

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
    val port = sc.conf.getOption("spark.ps.driver.port").getOrElse("7777").toInt
    val ioEncryptionKey = if (sc.conf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(sc.conf))
    } else {
      None
    }
    logInfo(s"setup ps driver rpc env: ${bindAddress}:${port} clientMode=${!isDriver}")
    RpcEnv.create("PSDriverEndpoint", bindAddress, port, sc.conf,
      sc.env.securityManager, clientMode = !isDriver)
  }

  def start() = {
    val env = createRpcEnv
    val pSDriverBackend = new PSDriverEndpoint(sc, env)
    psDriverRpcEndpointRef = env.setupEndpoint("ps-driver-endpoint", pSDriverBackend)
  }

}


