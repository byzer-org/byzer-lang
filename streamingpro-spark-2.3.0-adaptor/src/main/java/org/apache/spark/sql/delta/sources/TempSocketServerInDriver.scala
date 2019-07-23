package org.apache.spark.sql.delta.sources

import java.io.DataInputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.sources.mysql.binlog.{BinLogSocketServerSerDer, ReportBinlogSocketServerHostAndPort, SocketServerInExecutor}

/**
  * 2019-06-16 WilliamZhu(allwefantasy@gmail.com)
  */
class TempSocketServerInDriver(context: AtomicReference[ReportBinlogSocketServerHostAndPort]) extends BinLogSocketServerSerDer with Logging {
  val (server, host, port) = SocketServerInExecutor.setupOneConnectionServer("driver-socket-server") { sock =>
    handleConnection(sock)
  }

  def handleConnection(socket: Socket): Unit = {
    val dIn = new DataInputStream(socket.getInputStream)
    val req = readRequest(dIn).asInstanceOf[ReportBinlogSocketServerHostAndPort]
    context.set(req)
  }
}
