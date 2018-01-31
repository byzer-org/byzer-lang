package org.apache.spark.ps.cluster

import org.apache.spark.SparkContext
import org.apache.spark.rpc.{RpcEndpointRef}
import org.apache.spark.scheduler.cluster.PSDriverEndpoint

/**
  * Created by allwefantasy on 30/1/2018.
  */
class PSDriverBackend(sc: SparkContext) {

  val conf = sc.conf
  var psDriverBackend: RpcEndpointRef = null

  def start() = {
    psDriverBackend = sc.env.rpcEnv.setupEndpoint("PSDriver", new PSDriverEndpoint(sc.env.rpcEnv, sc))
  }

}


