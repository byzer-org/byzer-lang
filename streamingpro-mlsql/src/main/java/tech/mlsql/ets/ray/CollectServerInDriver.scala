package tech.mlsql.ets.ray

import java.io.DataInputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.MLSQLSparkUtils
import org.apache.spark.internal.Logging
import tech.mlsql.common.utils.distribute.socket.server.{ReportHostAndPort, ReportSingleAction, SocketServerInExecutor, SocketServerSerDer}
import tech.mlsql.common.utils.network.NetUtils

import scala.collection.mutable.ArrayBuffer

class CollectServerInDriver(context: AtomicReference[ArrayBuffer[ReportHostAndPort]], stopFlag: AtomicReference[String]) extends Logging {

  def host: String = if (MLSQLSparkUtils.rpcEnv().address == null) NetUtils.getHost
  else MLSQLSparkUtils.rpcEnv().address.host

  val (_server, _host, _port) = SocketServerInExecutor.setupOneConnectionServer(host, "driver-temp-socket-server") { sock =>
    handleConnection(sock)
  }

  def handleConnection(socket: Socket): Unit = {
    val dIn = new DataInputStream(socket.getInputStream)
    val client = new SocketServerSerDer[ReportSingleAction, ReportSingleAction]() {}
    val req = client.readRequest(dIn).asInstanceOf[ReportHostAndPort]
    context.get() += req
  }

  def close = {
    _server.close()
  }

}
