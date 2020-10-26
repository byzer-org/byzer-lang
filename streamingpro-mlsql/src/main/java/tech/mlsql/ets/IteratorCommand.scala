package tech.mlsql.ets

import java.net.ServerSocket
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import _root_.streaming.dsl.ScriptSQLExec
import _root_.streaming.dsl.mmlib.SQLAlg
import _root_.streaming.dsl.mmlib.algs.param.WowParams
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark._
import _root_.streaming.core.datasource.util.MLSQLJobCollect
import _root_.streaming.log.WowLog
import tech.mlsql.arrow.python.ispark.SparkContextImp
import tech.mlsql.arrow.python.runner.SparkSocketRunner
import tech.mlsql.common.utils.base.TryTool
import tech.mlsql.common.utils.distribute.socket.server.{ReportHostAndPort, SocketServerInExecutor}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.net.NetTool
import tech.mlsql.ets.ray.{CollectServerInDriver, DataServer}
import tech.mlsql.log.WriteLog
import tech.mlsql.version.VersionCompatibility

import scala.collection.mutable.ArrayBuffer

/**
 * !iterator _ -input tableName;
 */
class IteratorCommand(override val uid: String) extends SQLAlg
  with VersionCompatibility
  with WowParams
  with Logging
  with WowLog {
  def this() = this(UUID.randomUUID().toString)

  override def train(_df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val context = ScriptSQLExec.context()
    val session = context.execListener.sparkSession
    val refs = new AtomicReference[ArrayBuffer[ReportHostAndPort]]()
    refs.set(ArrayBuffer[ReportHostAndPort]())
    val stopFlag = new AtomicReference[String]()
    stopFlag.set("false")
    val tempSocketServerInDriver = new CollectServerInDriver(refs, stopFlag)

    val df = session.table(params("input"))
    var targetLen = df.rdd.partitions.length


    val tempdf = TryTool.tryOrElse {
      val resource = new SparkInstanceService(session).resources
      val jobInfo = new MLSQLJobCollect(session, context.owner)
      val leftResource = resource.totalCores - jobInfo.resourceSummary(null).activeTasks
      logInfo(s"Iterator: Resource:[${leftResource}(${resource.totalCores}-${jobInfo.resourceSummary(null).activeTasks})] TargetLen:[${targetLen}]")
      if (leftResource / 2 <= targetLen) {
        df.repartition(Math.max(Math.floor(leftResource / 2) - 1, 1).toInt)
      } else df
    } {
      //      WriteLog.write(List("Warning: Fail to detect instance resource. Setup 4 data server for Python.").toIterator, runnerConf)
      logWarning(format("Warning: Fail to detect instance resource. Setup 4 data server for Python."))
      if (targetLen > 4) {
        df.repartition(4)
      } else df
    }

    targetLen = tempdf.rdd.partitions.length
    logInfo(s"Iterator: Final TargetLen ${targetLen}")
    val _owner = ScriptSQLExec.context().owner

    val thread = new Thread("temp-data-server-in-spark") {
      override def run(): Unit = {

        val dataSchema = df.schema
        val tempSocketServerHost = tempSocketServerInDriver._host
        val tempSocketServerPort = tempSocketServerInDriver._port
        val timezoneID = session.sessionState.conf.sessionLocalTimeZone
        val owner = _owner
        tempdf.rdd.mapPartitions { iter =>

          val host: String = if (SparkEnv.get == null || MLSQLSparkUtils.blockManager == null || MLSQLSparkUtils.blockManager.blockManagerId == null) {
            WriteLog.write(List("Iterator: Cannot get MLSQLSparkUtils.rpcEnv().address, using NetTool.localHostName()").iterator,
              Map("PY_EXECUTE_USER" -> owner))
            NetTool.localHostName()
          }
          else MLSQLSparkUtils.blockManager.blockManagerId.host

          val socketRunner = new SparkSocketRunner("serveToStreamWithArrow", host, timezoneID)
          val commonTaskContext = new SparkContextImp(TaskContext.get(), null)
          val convert = WowRowEncoder.fromRow(dataSchema)
          val newIter = iter.map { irow =>
            convert(irow)
          }
          val Array(_server, _host, _port) = socketRunner.serveToStreamWithArrow(newIter, dataSchema, 1000, commonTaskContext)

          // send server info back
          SocketServerInExecutor.reportHostAndPort(tempSocketServerHost,
            tempSocketServerPort,
            ReportHostAndPort(_host.toString, _port.toString.toInt))

          while (_server != null && !_server.asInstanceOf[ServerSocket].isClosed) {
            Thread.sleep(1 * 1000)
          }
          List[String]().iterator
        }.count()
        logInfo("Exit all data server")
      }
    }
    thread.setDaemon(true)
    thread.start()

    var clockTimes = 120
    while (targetLen != refs.get().length && clockTimes >= 0) {
      Thread.sleep(500)
      clockTimes -= 1
    }
    if (clockTimes < 0) {
      throw new RuntimeException(s"fail to start data socket server. targetLen:${targetLen} actualLen:${refs.get().length}")
    }
    tempSocketServerInDriver.shutdown
    val timezoneID = session.sessionState.conf.sessionLocalTimeZone
//    val dataModeSchema = StructType(Seq(StructField("host", StringType), StructField("port", LongType), StructField("server_id", StringType)))
    import session.implicits._
    val newdf = session.createDataset[DataServer](refs.get().map(f => DataServer(f.host, f.port, timezoneID))).repartition(1)
    newdf.toDF()
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
  }
}
