package org.apache.spark.ps.cluster

import java.net.URL
import java.util.Locale

import _root_.streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import org.apache.spark._
import org.apache.spark.api.MLSQLExecutorPlugin
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.util.ThreadUtils
import tech.mlsql.common.utils.exception.ExceptionTool
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.log.WriteLog
import tech.mlsql.python.BasicCondaEnvManager

import scala.collection.mutable
import scala.util.{Failure, Success}

/**
 * 26/2/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class PSExecutorBackend(env: SparkEnv, override val rpcEnv: RpcEnv, psDriverUrl: String, psExecutorId: String, hostname: String, cores: Int) extends ThreadSafeRpcEndpoint with Logging {

  override def onStart(): Unit = {

    if (PSExecutorBackend.isLocalMaster(env.conf)) {
      val runtime = PlatformManager.getRuntime
      while (runtime.asInstanceOf[SparkRuntime].psDriverBackend == null &&
        runtime.asInstanceOf[SparkRuntime].psDriverBackend.psDriverRpcEndpointRef != null) {
        Thread.sleep(500)
        logInfo("waiting psDriverBackend ready.")
      }
    }
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
    case Message.CopyModelToLocal(modelPath, destPath) => {
      logInfo(s"copying model: ${modelPath} -> ${destPath}")
      HDFSOperator.copyToLocalFile(destPath, modelPath, true)
      context.reply(true)
    }

    case Message.CreateOrRemovePythonEnv(user, groupId, condaYamlFile, options, command) => {
      var message = ""
      val success = try {
        val condaEnvManager = new BasicCondaEnvManager(user, groupId, context.senderAddress.hostPort, options)
        command match {
          case Message.AddEnvCommand =>
            condaEnvManager.getOrCreateCondaEnv(Option(condaYamlFile))
          case Message.RemoveEnvCommand =>
            condaEnvManager.removeEnv(Option(condaYamlFile))
        }
        true
      } catch {
        case e: Exception =>
          logError("Create PythonEnv fail", e)
          message = ExceptionTool.exceptionString(e)
          false
      }

      context.reply((success, message))
    }
    case Message.Ping =>
      logInfo(s"received message ${Message.Ping}")
      context.reply(Message.Pong(psExecutorId))
  }
}

object PSExecutorBackend {

  def isLocalMaster(conf: SparkConf): Boolean = {
    //      val master = MLSQLConf.MLSQL_MASTER.readFrom(configReader).getOrElse("")
    val master = conf.get("spark.master", "")
    master == "local" || master.startsWith("local[")
  }

  var executorBackend: Option[PSExecutorBackend] = None

  def loadPlugin(conf: SparkConf): Unit = {
    val env = SparkEnv.get
    var psDriverUrl: String = null
    var psExecutorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var appId: String = null
    var psDriverPort = 7777
    var psDriverHost: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()

    def parseArgs = {
      //val runtimeMxBean = ManagementFactory.getRuntimeMXBean();
      //var argv = runtimeMxBean.getInputArguments.toList
      var argv = System.getProperty("sun.java.command").split("\\s+").toList

      var count = 0
      var first = 0
      argv.foreach { f =>
        if (f.startsWith("--") && first == 0) {
          first = count
        }
        count += 1
      }
      argv = argv.drop(first)

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
      if (psDriverUrl.contains("@")) {
        psDriverUrl = psDriverUrl.split("@").last
      }
      val Array(host, port) = psDriverUrl.split(":")
      psDriverHost = host
      // this port have been set in SparkRuntime dynamically
      psDriverPort = env.conf.getInt("spark.ps.cluster.driver.port", 0)
      if (psDriverPort == 0) {
        throw new RuntimeException("Executor psDriverPort can not get spark.ps.cluster.driver.port")
      }
      psDriverUrl = "spark://ps-driver-endpoint@" + psDriverHost + ":" + psDriverPort
    }

    if (!isLocalMaster(conf)) {
      parseArgs
    } else {
      psDriverPort = env.conf.getInt("spark.ps.cluster.driver.port", 0)
      hostname = conf.get(DRIVER_BIND_ADDRESS)
      psDriverUrl = "spark://ps-driver-endpoint@" + hostname + ":" + psDriverPort
      psExecutorId = "0"
      cores = 1
    }

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

      RpcEnv.create("PSExecutorBackend", bindAddress, port, env.conf,
        env.securityManager, clientMode = !isDriver)
    }

    val rpcEnv = createRpcEnv
    val pSExecutorBackend = new PSExecutorBackend(env, rpcEnv, psDriverUrl, psExecutorId, hostname, cores)
    PSExecutorBackend.executorBackend = Some(pSExecutorBackend)
    rpcEnv.setupEndpoint("ps-executor-endpoint", pSExecutorBackend)
  }
}

class PSExecutorPlugin(conf: SparkConf) extends MLSQLExecutorPlugin with Logging {
  override def _init(config: Map[Any, Any]): Unit = {
    logInfo("PSExecutorPlugin starting.....")
    try {
      WriteLog.init(conf.getAll.toMap)
    } catch {
      case e: Exception => logInfo("Fail to connect DriverLogServer", e)
    }
    try {
      PSExecutorBackend.loadPlugin(conf)
    } catch {
      case e: Exception => logInfo("Fail to load PSExecutorPlugin", e)
    }


  }


  override def _shutdown(): Unit = {
  }
}
