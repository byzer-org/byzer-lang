package org.apache.spark.ps.cluster

import streaming.tensorflow.TFModelLoader
import java.util.Locale

import org.apache.spark.internal.Logging
import org.apache.spark.SparkEnv
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.util.ThreadUtils
import streaming.common.HDFSOperator

import scala.util.{Failure, Success}


/**
  * Created by allwefantasy on 30/1/2018.
  */
class PSExecutorBackend(env: SparkEnv, override val rpcEnv: RpcEnv, psDriverUrl: String, psExecutorId: String, hostname: String, cores: Int) extends ThreadSafeRpcEndpoint with Logging {

  override def onStart(): Unit = {

    logInfo("Connecting to driver: " + psDriverUrl)
    rpcEnv.asyncSetupEndpointRefByURI(psDriverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      val driver = Some(ref)
      ref.ask[Boolean](Message.RegisterPSExecutor(psExecutorId, self, hostname, cores, extractLogUrls))
    }(ThreadUtils.sameThread).onComplete {
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      case Success(msg) =>
        logInfo(s"${psExecutorId}@${hostname} register with driver: $psDriverUrl success")
      // Always receive `true`. Just ignore it
      case Failure(e) =>
        logError(s"Cannot register with driver: $psDriverUrl", e)
    }(ThreadUtils.sameThread)
  }

  def extractLogUrls: Map[String, String] = {
    val prefix = "SPARK_LOG_URL_"
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toLowerCase(Locale.ROOT), e._2))
  }

  override def receive: PartialFunction[Any, Unit] = {
    case Message.RegisteredExecutor =>
    case Message.RegisterExecutorFailed =>

  }


  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case Message.TensorFlowModelClean(modelPath) => {
      logInfo("clean tensorflow model")
      TFModelLoader.close(modelPath)
      context.reply(true)
    }
    case Message.CopyModelToLocal(modelPath, destPath) => {
      logInfo(s"copying model: ${modelPath} -> ${destPath}")
      HDFSOperator.copyToLocalFile(destPath, modelPath, true)
      context.reply(true)
    }
  }
}

object PSExecutorBackend {
  var executorBackend: Option[PSExecutorBackend] = None
}


