package org.apache.spark.ps.cluster

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RegisteredExecutor

/**
  * Created by allwefantasy on 30/1/2018.
  */
object Message {

  case class RefreshPSExecutors()

  case class RegisteredExecutor()

  case class RegisterExecutorFailed(msg: String)

  case class RegisterPSExecutor(
                                 executorId: String,
                                 executorRef: RpcEndpointRef,
                                 hostname: String,
                                 cores: Int,
                                 logUrls: Map[String, String])

  case class TensorFlowModelClean(modelPath: String)

  case class CopyModelToLocal(modelPath: String, destPath: String)

}
