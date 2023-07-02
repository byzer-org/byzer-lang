package tech.mlsql.tool

import java.net.{InetAddress, ServerSocket}
import java.util.concurrent.atomic.AtomicReference

import _root_.streaming.core.datasource.util.MLSQLJobCollect
import _root_.streaming.dsl.ScriptSQLExec
import org.apache.spark._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions => f}
import tech.mlsql.arrow.python.ispark.SparkContextImp
import tech.mlsql.arrow.python.runner.SparkSocketRunner
import tech.mlsql.common.utils.base.TryTool
import tech.mlsql.common.utils.distribute.socket.server.{ReportHostAndPort, SocketServerInExecutor}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.net.NetTool
import tech.mlsql.ets.ray.CollectServerInDriver
import tech.mlsql.log.WriteLog

import scala.collection.mutable.ArrayBuffer

/**
 * Start new  server in driver and then start data servers in tasks.
 * Once the data servers are started, the will report their
 * host:port to driver server. So we can deliver this information
 * to Python jobs.
 */
class MasterSlaveInSpark(name: String, session: SparkSession, _owner: String) extends Logging {

  private var tempSocketServerInDriver: CollectServerInDriver[String] = null
  private var refs: AtomicReference[ArrayBuffer[ReportHostAndPort]] = null
  private var targetLen: Int = 0

  def build(df: DataFrame, config: Map[String, String], job: (String,
    String,
    StructType,
    Iterator[Row],
    String,
    Int) => Unit) = {
    val (_targetLen, shouldSort) = computeSplits(df)

    val keepPartitionNum = config.getOrElse("keepPartitionNum", "false").toBoolean
    if (!keepPartitionNum) {
      targetLen = _targetLen
    }else{
      targetLen = df.rdd.partitions.length
    }

    val tempdf = if (!shouldSort) {
      if (!keepPartitionNum) {
        df.repartition(targetLen)
      } else df
    } else df.repartition(1).sortWithinPartitions(f.col("start").asc)

    buildDataSocketServers(tempdf, job)
    this
  }

  def dataServers = {
    assert(refs != null, "invoke build first")
    refs
  }

  def waitWithTimeout(seconds: Int = 60, msg: String = "") = {
    assert(tempSocketServerInDriver != null, "invoke build first")
    var clockTimes = seconds * 2
    while (targetLen != refs.get().length && clockTimes >= 0) {
      Thread.sleep(500)
      clockTimes -= 1
    }
    if (clockTimes < 0) {
      tempSocketServerInDriver.shutdown

      val resource = new SparkInstanceService(session).resources
      val context = ScriptSQLExec.context()
      val jobInfo = new MLSQLJobCollect(session, context.owner)
      val totalCores = resource.totalCores

      val suggestMsg = if (totalCores <= targetLen) {
        s"The total cores is ${totalCores}, but we try to start ${targetLen} servers, please try to increase the Byzer CPU resource."
      } else ""

      throw new RuntimeException(
        s"""
           |Fail to start socket server(${msg}) within ${seconds} seconds.
           |The system try to start [${targetLen}] servers, but only ${refs.get().length} is started.
           |${suggestMsg}
           |""".stripMargin)
    }
    tempSocketServerInDriver.shutdown
    this
  }

  private def computeSplits(df: DataFrame) = {
    var targetLen = df.rdd.partitions.length
    var sort = false
    val context = ScriptSQLExec.context()

    MLSQLSparkUtils.isFileTypeTable(df) match {
      case true =>
        targetLen = 1
        sort = true
      case false =>
        TryTool.tryOrElse {
          val resource = new SparkInstanceService(session).resources
          val jobInfo = new MLSQLJobCollect(session, context.owner)
          val totalCores = resource.totalCores
          val activeCores = jobInfo.resourceSummary(null).activeTasks
          val freeCores = totalCores - activeCores
          logInfo(s"Detect resources: totalCores:${totalCores} activeCores:${activeCores} freeCores:${freeCores}")
          logInfo("Compute formula: freeCores / 2 <= targetLen ? targetLen = Math.max(Math.floor(freeCores / 2) - 1, 1).toInt : targetLen = targetLen")
          if (freeCores / 2 <= targetLen) {
            targetLen = Math.max(Math.floor(freeCores / 2) - 1, 1).toInt
          }
          logInfo(s"Setup [${targetLen}] data refs in Python side.")
        } {
          logWarning("Fail to detect instance resource. Setup 4 data refs for Python side")
          if (targetLen > 4) targetLen = 4
        }
    }

    (targetLen, sort)
  }


  private def buildDataSocketServers(tempdf: DataFrame, job: (String,
    String,
    StructType,
    Iterator[Row],
    String,
    Int) => Unit) = {

    refs = new AtomicReference[ArrayBuffer[ReportHostAndPort]]()
    refs.set(ArrayBuffer[ReportHostAndPort]())
    val stopFlag = new AtomicReference[String]()
    stopFlag.set("false")

    tempSocketServerInDriver = new CollectServerInDriver(refs, stopFlag)

    val thread = new Thread(name) {
      override def run(): Unit = {
        //make sure the thread reassign any variable ref  out of the scope.
        //Otherwise spark will fail with task serialization
        val _job = job
        val dataSchema = tempdf.schema
        val tempSocketServerHost = tempSocketServerInDriver._host
        val tempSocketServerPort = tempSocketServerInDriver._port
        val timezoneID = session.sessionState.conf.sessionLocalTimeZone
        val owner = _owner
        tempdf.rdd.mapPartitions { iter =>

          val host: String = if (SparkEnv.get == null || MLSQLSparkUtils.blockManager == null || MLSQLSparkUtils.blockManager.blockManagerId == null) {
            WriteLog.write(List("Ray: Cannot get MLSQLSparkUtils.rpcEnv().address, using NetTool.localHostName()").iterator,
              Map("PY_EXECUTE_USER" -> owner))
            NetTool.localHostName()
          } else if (SparkEnv.get != null && SparkEnv.get.conf.getBoolean("spark.mlsql.deploy.on.k8s", false)) {
            InetAddress.getLocalHost.getHostAddress
          }
          else MLSQLSparkUtils.blockManager.blockManagerId.host
          _job(host, timezoneID, dataSchema, iter, tempSocketServerHost, tempSocketServerPort)
          List[String]().iterator
        }.collect()
        logInfo("Exit all data server")
      }
    }
    thread.setDaemon(true)
    thread.start()
  }
}

object MasterSlaveInSpark {
  def defaultDataServerImpl(
                             host: String,
                             timezoneID: String,
                             dataSchema: StructType,
                             iter: Iterator[Row],
                             driverSocketServerHost: String,
                             driverSocketServerPort: Int
                           ): Unit = {
    val socketRunner = new SparkSocketRunner("serveToStreamWithArrow", host, timezoneID)
    val commonTaskContext = new SparkContextImp(TaskContext.get(), null)
    val convert = WowRowEncoder.fromRow(dataSchema)
    val newIter = iter.map { irow =>
      convert(irow)
    }
    val Array(_server, _host, _port) = socketRunner.serveToStreamWithArrow(newIter, dataSchema, 1000, commonTaskContext)

    // send server info back
    SocketServerInExecutor.reportHostAndPort(driverSocketServerHost,
      driverSocketServerPort,
      ReportHostAndPort(_host.toString, _port.toString.toInt))

    while (_server != null && !_server.asInstanceOf[ServerSocket].isClosed) {
      Thread.sleep(1 * 1000)
    }
  }
}
