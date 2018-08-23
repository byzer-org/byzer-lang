package org.apache.spark.scheduler.local

import java.io.File
import java.net.URL
import java.nio.ByteBuffer

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TaskState}
import org.apache.spark.TaskState._
import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.{SchedulerBackend, SparkListenerExecutorAdded, TaskSchedulerImpl, WorkerOffer}
import org.apache.spark.scheduler.cluster.ExecutorInfo

/**
  * Created by allwefantasy on 5/8/2018.
  */

private[spark] class WowLocalEndpoint(
                                       override val rpcEnv: RpcEnv,
                                       userClassPath: Seq[URL],
                                       scheduler: TaskSchedulerImpl,
                                       executorBackend: WowLocalSchedulerBackend,
                                       private val totalCores: Int)
  extends ThreadSafeRpcEndpoint with Logging {

  private var freeCores = totalCores

  val localExecutorId = SparkContext.DRIVER_IDENTIFIER
  val localExecutorHostname = "localhost"

  private val executor = new Executor(
    localExecutorId, localExecutorHostname, SparkEnv.get, userClassPath, isLocal = true)

  override def receive: PartialFunction[Any, Unit] = {
    case ReviveOffers =>
      reviveOffers()

    case StatusUpdate(taskId, state, serializedData) =>
      scheduler.statusUpdate(taskId, state, serializedData)
      if (TaskState.isFinished(state)) {
        freeCores += scheduler.CPUS_PER_TASK
        reviveOffers()
      }

    case KillTask(taskId, interruptThread, reason) =>
      executor.killTask(taskId, interruptThread, reason)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case StopExecutor =>
      executor.stop()
      context.reply(true)
  }

  def reviveOffers() {
    val offers = IndexedSeq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores))
    for (task <- scheduler.resourceOffers(offers).flatten) {
      freeCores -= scheduler.CPUS_PER_TASK
      executor.launchTask(executorBackend, task)
    }
  }
}

class WowLocalSchedulerBackend(
                                _conf: SparkConf,
                                scheduler: TaskSchedulerImpl,
                                val totalCores: Int)
  extends SchedulerBackend with ExecutorBackend with Logging {

  private val appId = "local-" + System.currentTimeMillis
  private var localEndpoint: RpcEndpointRef = null
  private val userClassPath = getUserClasspath(_conf)
  private val listenerBus = scheduler.sc.listenerBus
  private val launcherBackend = new LauncherBackend() {
    override def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)

    override protected def conf: SparkConf = _conf
  }

  /**
    * Returns a list of URLs representing the user classpath.
    *
    * @param conf Spark configuration.
    */
  def getUserClasspath(conf: SparkConf): Seq[URL] = {
    val userClassPathStr = conf.getOption("spark.executor.extraClassPath")
    userClassPathStr.map(_.split(File.pathSeparator)).toSeq.flatten.map(new File(_).toURI.toURL)
  }

  launcherBackend.connect()

  override def start() {
    scheduler.backend = this
    val rpcEnv = SparkEnv.get.rpcEnv
    val executorEndpoint = new WowLocalEndpoint(rpcEnv, userClassPath, scheduler, this, totalCores)
    localEndpoint = rpcEnv.setupEndpoint("WowLocalSchedulerBackendEndpoint", executorEndpoint)
    listenerBus.post(SparkListenerExecutorAdded(
      System.currentTimeMillis,
      executorEndpoint.localExecutorId,
      new ExecutorInfo(executorEndpoint.localExecutorHostname, totalCores, Map.empty)))
    launcherBackend.setAppId(appId)
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  override def stop() {
    stop(SparkAppHandle.State.FINISHED)
  }

  override def reviveOffers() {
    localEndpoint.send(ReviveOffers)
  }

  override def defaultParallelism(): Int =
    scheduler.conf.getInt("spark.default.parallelism", totalCores)

  override def killTask(
                         taskId: Long, executorId: String, interruptThread: Boolean, reason: String) {
    localEndpoint.send(KillTask(taskId, interruptThread, reason))
  }

  override def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer) {
    localEndpoint.send(StatusUpdate(taskId, state, serializedData))
  }

  override def applicationId(): String = appId

  private def stop(finalState: SparkAppHandle.State): Unit = {
    localEndpoint.ask(StopExecutor)
    try {
      launcherBackend.setState(finalState)
    } finally {
      launcherBackend.close()
    }
  }

}
