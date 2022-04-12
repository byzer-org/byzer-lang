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

  def build(df: DataFrame, job: (String,
    String,
    StructType,
    Iterator[Row],
    String,
    Int,
    Int) => Unit) = {
    val (_targetLen, shouldSort) = computeSplits(df)
    targetLen = _targetLen

    val tempdf = if (!shouldSort) {
      df.repartition(targetLen)
    } else df.repartition(1).sortWithinPartitions(f.col("start").asc)

    buildDataSocketServers(tempdf, job)
    this
  }

  def build(df: DataFrame, job: (String,
    String,
    StructType,
    Iterator[Row],
    String,
    Int,
    Int) => Unit, runnerConf: Map[String, String]) = {
    val (_targetLen, shouldSort) = computeSplits(df)
    targetLen = _targetLen

    val tempdf = if (!shouldSort) {
      df.repartition(targetLen)
    } else df.repartition(1).sortWithinPartitions(f.col("start").asc)

    buildDataSocketServers(tempdf, job, runnerConf)
    this
  }

  def dataServers = {
    assert(refs != null, "invoke build first")
    refs
  }

  def waitWithTimeout(seconds: Int = 60) = {
    assert(tempSocketServerInDriver != null, "invoke build first")
    var clockTimes = seconds * 2
    while (targetLen != refs.get().length && clockTimes >= 0) {
      Thread.sleep(500)
      clockTimes -= 1
    }
    if (clockTimes < 0) {
      tempSocketServerInDriver.shutdown
      throw new RuntimeException(s"fail to start data socket server. targetLen:${targetLen} actualLen:${refs.get().length}")
    }
    tempSocketServerInDriver.shutdown
    this
  }

  def computeSplits(df: DataFrame) = {
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
          val leftResource = resource.totalCores - jobInfo.resourceSummary(null).activeTasks
          logInfo(s"RayMode: Resource:[${leftResource}(${resource.totalCores}-${jobInfo.resourceSummary(null).activeTasks})] TargetLen:[${targetLen}]")
          if (leftResource / 2 <= targetLen) {
            targetLen = Math.max(Math.floor(leftResource / 2) - 1, 1).toInt
          }
        } {
          logWarning("Warning: Fail to detect instance resource. Setup 4 data server for Python.")
          if (targetLen > 4) targetLen = 4
        }
    }

    (targetLen, sort)
  }

  def getRefs(): AtomicReference[ArrayBuffer[ReportHostAndPort]] = {
    return refs
  }


  private def buildDataSocketServers(tempdf: DataFrame, job: (String,
    String,
    StructType,
    Iterator[Row],
    String,
    Int,
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
        val maxIterCount = session.conf.get("maxIterCount", "1000").toInt
        tempdf.rdd.mapPartitions { iter =>

          val host: String = if (SparkEnv.get == null || MLSQLSparkUtils.blockManager == null || MLSQLSparkUtils.blockManager.blockManagerId == null) {
            WriteLog.write(List("Ray: Cannot get MLSQLSparkUtils.rpcEnv().address, using NetTool.localHostName()").iterator,
              Map("PY_EXECUTE_USER" -> owner))
            NetTool.localHostName()
          } else if (SparkEnv.get != null && SparkEnv.get.conf.getBoolean("spark.mlsql.deploy.on.k8s", false)) {
            InetAddress.getLocalHost.getHostAddress
          }
          else MLSQLSparkUtils.blockManager.blockManagerId.host
          _job(host, timezoneID, dataSchema, iter, tempSocketServerHost, tempSocketServerPort, maxIterCount)
          List[String]().iterator
        }.collect()
        logInfo("Exit all data server")
      }
    }
    thread.setDaemon(true)
    thread.start()
  }

  private def buildDataSocketServers(tempdf: DataFrame, job: (String,
    String,
    StructType,
    Iterator[Row],
    String,
    Int,
    Int) => Unit, runnerConf: Map[String, String]) = {

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
        val maxIterCount = runnerConf.getOrElse("maxIterCount", "1000").toInt
        tempdf.rdd.mapPartitions { iter =>

          val host: String = if (SparkEnv.get == null || MLSQLSparkUtils.blockManager == null || MLSQLSparkUtils.blockManager.blockManagerId == null) {
            WriteLog.write(List("Ray: Cannot get MLSQLSparkUtils.rpcEnv().address, using NetTool.localHostName()").iterator,
              Map("PY_EXECUTE_USER" -> owner))
            NetTool.localHostName()
          } else if (SparkEnv.get != null && SparkEnv.get.conf.getBoolean("spark.mlsql.deploy.on.k8s", false)) {
            InetAddress.getLocalHost.getHostAddress
          }
          else MLSQLSparkUtils.blockManager.blockManagerId.host
          _job(host, timezoneID, dataSchema, iter, tempSocketServerHost, tempSocketServerPort, maxIterCount)
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
  def defaultDataServerWithIterCountImpl(
                                          host: String,
                                          timezoneID: String,
                                          dataSchema: StructType,
                                          iter: Iterator[Row],
                                          driverSocketServerHost: String,
                                          driverSocketServerPort: Int,
                                          maxIterCount: Int
                                        ): Unit = {
    val socketRunner = new SparkSocketRunner("serveToStreamWithArrow", host, timezoneID)
    val commonTaskContext = new SparkContextImp(TaskContext.get(), null)
    val convert = WowRowEncoder.fromRow(dataSchema)
    val newIter = iter.map { irow =>
      convert(irow)
    }
    val Array(_server, _host, _port) = socketRunner.serveToStreamWithArrow(newIter, dataSchema, maxIterCount, commonTaskContext)

    // send server info back
    SocketServerInExecutor.reportHostAndPort(driverSocketServerHost,
      driverSocketServerPort,
      ReportHostAndPort(_host.toString, _port.toString.toInt))

    while (_server != null && !_server.asInstanceOf[ServerSocket].isClosed) {
      Thread.sleep(1 * 1000)
    }
  }

  def defaultDataServerImpl(
                             host: String,
                             timezoneID: String,
                             dataSchema: StructType,
                             iter: Iterator[Row],
                             driverSocketServerHost: String,
                             driverSocketServerPort: Int
                           ): Unit = {
    defaultDataServerWithIterCountImpl(host, timezoneID, dataSchema, iter, driverSocketServerHost, driverSocketServerPort, 1000)
  }
}
