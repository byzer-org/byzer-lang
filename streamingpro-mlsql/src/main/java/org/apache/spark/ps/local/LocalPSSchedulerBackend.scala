package org.apache.spark.ps.local

import streaming.tensorflow.TFModelLoader
import java.io.File
import java.net.URL

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.ps.cluster.Message
import org.apache.spark.ps.cluster.Message.CopyModelToLocal
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.StopExecutor
import streaming.common.HDFSOperator


private case class TensorFlowModelClean(modelPath: String)


/**
  * Calls to [[LocalPSSchedulerBackend]] are all serialized through LocalEndpoint. Using an
  * RpcEndpoint makes the calls on [[LocalPSSchedulerBackend]] asynchronous, which is necessary
  * to prevent deadlock between [[LocalPSSchedulerBackend]] and the [[TaskSchedulerImpl]].
  */
class LocalPSEndpoint(override val rpcEnv: RpcEnv,
                      userClassPath: Seq[URL]

                     )
  extends ThreadSafeRpcEndpoint with Logging {

  val localExecutorId = SparkContext.DRIVER_IDENTIFIER
  val localExecutorHostname = "localhost"

  override def receive: PartialFunction[Any, Unit] = {
    case _ =>

  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case TensorFlowModelClean(modelPath) =>
      logInfo("close tensorflow model: " + modelPath)
      TFModelLoader.close(modelPath)
      context.reply(true)
    case Message.CopyModelToLocal(modelPath, destPath) => {
      logInfo(s"copying model: ${modelPath} -> ${destPath}")
      HDFSOperator.copyToLocalFile(destPath, modelPath, true)
      context.reply(true)
    }
  }
}


class LocalPSSchedulerBackend(sparkContext: SparkContext)
  extends Logging {

  private val appId = "local-ps-" + System.currentTimeMillis
  var localEndpoint: RpcEndpointRef = null
  private val userClassPath = getUserClasspath(sparkContext.getConf)
  private val launcherBackend = new LauncherBackend() {
    override def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)

    protected def conf: SparkConf = sparkContext.conf
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

  def start() {
    val rpcEnv = SparkEnv.get.rpcEnv
    val executorEndpoint = new LocalPSEndpoint(rpcEnv, userClassPath)
    localEndpoint = rpcEnv.setupEndpoint("PSLocalSchedulerBackend", executorEndpoint)
    launcherBackend.setAppId(appId)
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
    LocalExecutorBackend.executorBackend = Some(this)
  }

  def stop() {
    stop(SparkAppHandle.State.FINISHED)
  }

  def cleanTensorFlowModel(modelPath: String) = {
    localEndpoint.askSync[Boolean](TensorFlowModelClean(modelPath))
  }

  private def stop(finalState: SparkAppHandle.State): Unit = {
    localEndpoint.ask(StopExecutor)
    try {
      launcherBackend.setState(finalState)
    } finally {
      launcherBackend.close()
    }
  }

}

object LocalExecutorBackend {
  var executorBackend: Option[LocalPSSchedulerBackend] = None
}