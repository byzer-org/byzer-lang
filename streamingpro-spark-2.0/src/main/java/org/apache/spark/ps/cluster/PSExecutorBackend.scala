package org.apache.spark.ps.cluster

import java.util.{Locale, Properties}

import com.codahale.metrics.MetricRegistry
import org.apache.spark.internal.Logging
import org.apache.spark.{SecurityManager, SparkContext, SparkEnv}
import org.apache.spark.metrics.sink.Sink
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.util.ThreadUtils
import streaming.tensorflow.TFModelLoader

import scala.util.{Failure, Success}
import java.lang.management.ManagementFactory
import java.net.URL

import org.apache.spark.internal.config._
import org.apache.spark.security.CryptoStreamUtils

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 30/1/2018.
  */
class PSExecutorBackend(env: SparkEnv) extends ThreadSafeRpcEndpoint with Logging {


  var psDriverUrl: String = null
  var psExecutorId: String = null
  var hostname: String = null
  var cores: Int = 0
  var appId: String = null
  val psDriverPort = 7777
  var psDriverHost: String = null
  var workerUrl: Option[String] = None
  val userClassPath = new mutable.ListBuffer[URL]()

  def createRpcEnv = {
    val isDriver = env.executorId == SparkContext.DRIVER_IDENTIFIER
    val bindAddress = hostname
    val advertiseAddress = ""
    val port = env.conf.getOption("spark.ps.executor.port").getOrElse("0").toInt
    val ioEncryptionKey = if (env.conf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(env.conf))
    } else {
      None
    }
    RpcEnv.create("PSExecutorBackend", bindAddress, advertiseAddress, port, env.conf,
      env.securityManager, clientMode = !isDriver)
  }


  def parseArgs = {
    val runtimeMxBean = ManagementFactory.getRuntimeMXBean();
    var argv = runtimeMxBean.getInputArguments().toList

    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          psDriverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          psExecutorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
      }
    }
    val Array(host, port) = psDriverUrl.split(":")
    psDriverHost = host
    psDriverUrl = psDriverHost + ":" + psDriverPort
  }

  parseArgs

  override val rpcEnv: RpcEnv = createRpcEnv

  override def onStart(): Unit = {

    logInfo("Connecting to driver: " + psDriverUrl)
    rpcEnv.asyncSetupEndpointRefByURI(psDriverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      val driver = Some(ref)
      ref.ask[Boolean](Message.RegisterPSExecutor(psExecutorId, self, hostname, cores, extractLogUrls))
    }(ThreadUtils.sameThread).onComplete {
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      case Success(msg) =>
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
      logInfo("ps executor get message: Message.TensorFlowModelClean")
      TFModelLoader.close(modelPath)
      context.reply(true)
    }
  }
}

class PSServiceSink(val property: Properties, val registry: MetricRegistry,
                    securityMgr: SecurityManager) extends Sink {
  val env = SparkEnv.get


  override def start(): Unit = {
    if (env.executorId != SparkContext.DRIVER_IDENTIFIER) {
      val executorBackend = new PSExecutorBackend(env)
      env.rpcEnv.setupEndpoint("PSExecutor", executorBackend)
    }
  }

  override def stop(): Unit = {

  }

  override def report(): Unit = {

  }
}
