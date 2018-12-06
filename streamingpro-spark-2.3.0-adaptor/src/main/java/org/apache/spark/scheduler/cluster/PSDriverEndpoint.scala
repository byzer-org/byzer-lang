package org.apache.spark.scheduler.cluster

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.ps.cluster.Message
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.util.{ThreadUtils, Utils}

import scala.collection.mutable.HashMap

/**
  * Created by allwefantasy on 31/1/2018.
  */
class PSDriverEndpoint(sc: SparkContext, override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging {
  protected val addressToExecutorId = new HashMap[RpcAddress, String]
  private val executorDataMap = new HashMap[String, ExecutorData]()
  //  private var sparkExecutorDataMap = new HashMap[String, ExecutorData]()
  //  private val refreshThread =
  ThreadUtils.newDaemonSingleThreadScheduledExecutor("ps-driver-refresh-thread")

  override def onStart() {
    // Periodically revive offers to allow delay scheduling to work
    logInfo("started PSDriverEndpoint")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case Message.TensorFlowModelClean(modelPath) =>
      val ks = sc.getExecutorIds().toSet
      logInfo(s"ps driver send message: Message.TensorFlowModelClean:executors:${ks}")
      executorDataMap.foreach { ed =>
        if (ks.contains(ed._1)) {
          ed._2.executorEndpoint.askSync[Boolean](Message.TensorFlowModelClean(modelPath))
        }
      }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case Message.RegisterPSExecutor(executorId, executorRef, hostname, cores, logUrls) =>
      if (executorDataMap.contains(executorId)) {
        executorRef.send(Message.RegisterExecutorFailed("Duplicate executor ID: " + executorId))
        context.reply(true)
      } else {
        val executorAddress = if (executorRef.address != null) {
          executorRef.address
        } else {
          context.senderAddress
        }
        logInfo(s"Registered ps-executor $executorRef ($executorAddress) with ID $executorId")
        addressToExecutorId(executorAddress) = executorId

        val data = new ExecutorData(executorRef, executorRef.address, hostname,
          cores, cores, logUrls)
        executorDataMap.put(executorId, data)
        executorRef.send(Message.RegisteredExecutor)
        // Note: some tests expect the reply to come after we put the executor in the map
        context.reply(true)
      }
    case Message.CopyModelToLocal(modelPath, destPath) =>
      val ks = sc.getExecutorIds().toSet
      executorDataMap.foreach { ed =>
        if (ks.contains(ed._1)) {
          ed._2.executorEndpoint.askSync[Boolean](Message.CopyModelToLocal(modelPath, destPath))
        }
      }
      context.reply(true)

  }


}
