package tech.mlsql.ets.python

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{MLSQLSparkUtils, SparkEnv, TaskContext}
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.runner.{ArrowPythonRunner, ChainedPythonFunctions, PythonFunction}
import tech.mlsql.common.utils.distribute.socket.server.{Request, Response, SocketServerInExecutor, SocketServerSerDer}
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
        case ExecuteCode(code, envs, schema) =>
          val outputSchema = SparkSimpleSchemaParser.parse(schema).asInstanceOf[StructType]
          val javaConext = new JavaContext

          val envs4j = new util.HashMap[String, String]()
          envs.foreach { case (a, b) => envs4j.put(a, b) }

          val dataSchema = StructType(Seq(StructField("value", StringType)))
          val enconder = RowEncoder.apply(dataSchema).resolveAndBind()

          val newIter = Seq().map { irow =>
            enconder.toRow(irow)
          }.iterator
          try {
            val arrowRunner = new ArrowPythonRunner(
              Seq(ChainedPythonFunctions(Seq(PythonFunction(
                code, envs4j, "python", "3.6")))), dataSchema,
              "GMT", Map()
            )

            val commonTaskContext = new AppContextImpl(javaConext, arrowRunner)
            val columnarBatchIter = arrowRunner.compute(Iterator(newIter), TaskContext.getPartitionId(), commonTaskContext)
            val items = columnarBatchIter.flatMap { batch =>
              batch.rowIterator.asScala
            }.map(f => f.copy().toSeq(outputSchema))
            client.sendResponse(dOut, ExecuteResult(items.toSeq))
            javaConext.markComplete
            javaConext.close
          } catch {
            case e: Exception =>
              client.sendResponse(dOut, ExecuteResult(Seq()))
              e.printStackTrace()
          }

      }
    }

  }

  override def host: String = {
    if (SparkEnv.get == null) {
      //When SparkEnv.get is null, the program may run in a test
      //So return local address would be ok.
      "127.0.0.1"
    } else {
      MLSQLSparkUtils.rpcEnv().address.host
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

case class ExecuteCode(code: String, envs: Map[String, String], schema: String) extends Request[PythonSocketRequest] {
  override def wrap: PythonSocketRequest = PythonSocketRequest(executeCode = this)
}

case class ExecuteResult(a: Seq[Seq[Any]]) extends Response[PythonSocketResponse] {
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
