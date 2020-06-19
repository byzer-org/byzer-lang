package tech.mlsql.ets.python

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkEnv, TaskContext, WowRowEncoder}
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.runner.{ArrowPythonRunner, ChainedPythonFunctions, PythonFunction}
import tech.mlsql.common.utils.distribute.socket.server.{Request, Response, SocketServerInExecutor, SocketServerSerDer}
import tech.mlsql.common.utils.network.NetUtils
import tech.mlsql.schema.parser.SparkSimpleSchemaParser

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * 2019-08-16 WilliamZhu(allwefantasy@gmail.com)
 */
class PythonServer[T](taskContextRef: AtomicReference[T])
  extends SocketServerInExecutor[T](taskContextRef, "python-socket-server-in-executor")
    with Logging {

  @volatile private var markClose: AtomicBoolean = new AtomicBoolean(false)
  private val connections = new ArrayBuffer[Socket]()
  val client = new PythonClient()

  override def close() = {
    // make sure we only close once
    if (markClose.compareAndSet(false, true)) {
      logInfo(s"Shutdown ${host}. This may caused by the task is killed.")
    }
  }

  def isClosed = markClose.get()

  override def handleConnection(socket: Socket): Unit = {
    connections += socket
    socket.setKeepAlive(true)
    val dIn = new DataInputStream(socket.getInputStream)
    val dOut = new DataOutputStream(socket.getOutputStream)

    while (true) {
      client.readRequest(dIn) match {
        case _: ShutDownPythonServer =>
          close()
        case _: StartPythonServer =>
        case ExecuteCode(code, envs, conf, timezoneID) =>
          val outputSchema = SparkSimpleSchemaParser.parse(conf("schema")).asInstanceOf[StructType]
          val javaConext = new JavaContext

          val envs4j = new util.HashMap[String, String]()
          envs.foreach { case (a, b) => envs4j.put(a, b) }

          val dataSchema = StructType(Seq(StructField("value", StringType)))
          val convert = WowRowEncoder.fromRow(dataSchema)

          val newIter = Seq[Row]().map { irow =>
            convert(irow)
          }.iterator
          try {
            val arrowRunner = new ArrowPythonRunner(
              Seq(ChainedPythonFunctions(Seq(PythonFunction(
                code, envs4j, "python", "3.6")))), dataSchema,
              timezoneID, conf
            )

            val commonTaskContext = new AppContextImpl(javaConext, arrowRunner)
            val columnarBatchIter = arrowRunner.compute(Iterator(newIter), TaskContext.getPartitionId(), commonTaskContext)
            val items = columnarBatchIter.flatMap { batch =>
              batch.rowIterator.asScala
            }.map(f => f.copy().toSeq(outputSchema))
            client.sendResponse(dOut, ExecuteResult(true, items.toList))
            javaConext.markComplete
            javaConext.close
          } catch {
            case e: Exception =>
              val buffer = ArrayBuffer[String]()
              format_full_exception(buffer, e, true)
              client.sendResponse(dOut, ExecuteResult(false, buffer.map(Seq(_))))

          }

      }
    }

    def format_throwable(e: Throwable, skipPrefix: Boolean = false) = {
      (e.toString.split("\n") ++ e.getStackTrace.map(f => f.toString)).map(f => f).toSeq.mkString("\n")
    }


    def format_full_exception(buffer: ArrayBuffer[String], e: Exception, skipPrefix: Boolean = true) = {
      var cause = e.asInstanceOf[Throwable]
      buffer += format_throwable(cause, skipPrefix)
      while (cause.getCause != null) {
        cause = cause.getCause
        buffer += "caused by：\n" + format_throwable(cause, skipPrefix)
      }

    }

  }

  override def host: String = {
    if (SparkEnv.get == null) {
      //When SparkEnv.get is null, the program may run in a test
      //So return local address would be ok.
      "127.0.0.1"
    } else {
      val hostName = tech.mlsql.common.utils.network.SparkExecutorInfo.getInstance.hostname
      if (hostName == null) NetUtils.getHost else hostName
    }
  }

}

class PythonClient extends SocketServerSerDer[PythonSocketRequest, PythonSocketResponse]


case class StartPythonServer() extends Request[PythonSocketRequest] {
  override def wrap: PythonSocketRequest = PythonSocketRequest(startPythonServer = this)
}

case class ShutDownPythonServer() extends Request[PythonSocketRequest] {
  override def wrap: PythonSocketRequest = PythonSocketRequest(shutDownPythonServer = this)
}

case class ExecuteCode(code: String, envs: Map[String, String], conf: Map[String, String], timezoneID: String) extends Request[PythonSocketRequest] {
  override def wrap: PythonSocketRequest = PythonSocketRequest(executeCode = this)
}

case class ExecuteResult(ok: Boolean, a: Seq[Seq[Any]]) extends Response[PythonSocketResponse] {
  override def wrap: PythonSocketResponse = PythonSocketResponse(executeResult = this)
}

case class PythonSocketResponse(executeResult: ExecuteResult = null) {
  def unwrap: Response[_] = {
    if (executeResult != null) executeResult
    else null
  }
}

case class PythonSocketRequest(
                                startPythonServer: StartPythonServer = null,
                                shutDownPythonServer: ShutDownPythonServer = null,
                                executeCode: ExecuteCode = null

                              ) {
  def unwrap: Request[_] = {
    if (startPythonServer != null) {
      startPythonServer
    } else if (shutDownPythonServer != null) {
      shutDownPythonServer
    } else if (executeCode != null)
      executeCode
    else {
      null
    }
  }
}
