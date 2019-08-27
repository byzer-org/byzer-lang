package tech.mlsql.ets.ml.cluster

import java.net.ServerSocket

import tech.mlsql.common.utils.network.NetUtils

/**
  * 2019-08-26 WilliamZhu(allwefantasy@gmail.com)
  */
object PortManager {
  def preTaken = {
    // we should pre-take the port and report to master
    val MIN_PORT_NUMBER = 2221
    val MAX_PORT_NUMBER = 6666
    val holdPort = NetUtils.availableAndReturn(MIN_PORT_NUMBER, MAX_PORT_NUMBER)

    if (holdPort == null) {
      throw new RuntimeException(s"Fail to create tensorflow cluster, maybe executor cannot bind port ")
    }

    holdPort
  }

  def getPort(ss: ServerSocket) = {
    ss.getLocalPort
  }

  def releasePreTaken(ss: ServerSocket) = {
    NetUtils.releasePort(ss)
  }
}
