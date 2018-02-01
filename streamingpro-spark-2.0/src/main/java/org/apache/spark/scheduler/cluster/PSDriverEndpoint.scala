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
class PSDriverEndpoint(sc: SparkContext)
  extends ThreadSafeRpcEndpoint with Logging {
  protected val addressToExecutorId = new HashMap[RpcAddress, String]
  private val executorDataMap = new HashMap[String, ExecutorData]()
  private var sparkExecutorDataMap = new HashMap[String, ExecutorData]()
  private val refreshThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("ps-driver-refresh-thread")


  override def onStart() {
    // Periodically revive offers to allow delay scheduling to work
    val refreshInterval = 1000

    refreshThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        Option(self).foreach(_.send(Message.RefreshPSExecutors))
      }
    }, 0, refreshInterval, TimeUnit.MILLISECONDS)
  }

  override def receive: PartialFunction[Any, Unit] = {
    case Message.RefreshPSExecutors =>
      //.executorDataMap
      val cgsb = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
      val field = CoarseGrainedSchedulerBackend.getClass.getDeclaredField("executorDataMap")
      field.setAccessible(true)
      sparkExecutorDataMap = field.get(cgsb).asInstanceOf[HashMap[String, ExecutorData]]

    case Message.TensorFlowModelClean(modelPath) =>
      val ks = sparkExecutorDataMap.keySet
      executorDataMap.foreach { ed =>
        if (ks.contains(ed._1)) {
          logInfo("ps driver send message: Message.TensorFlowModelClean")
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
        executorRef.send(Message.RegisteredExecutor)
        // Note: some tests expect the reply to come after we put the executor in the map
        context.reply(true)
      }


  }

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
    RpcEnv.create("PSDriverEndpoint", bindAddress, advertiseAddress, port, sc.conf,
      sc.env.securityManager, clientMode = !isDriver)
  }

  override val rpcEnv: RpcEnv = createRpcEnv
}
