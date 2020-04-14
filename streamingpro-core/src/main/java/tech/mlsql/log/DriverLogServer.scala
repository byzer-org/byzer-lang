package tech.mlsql.log

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import org.apache.spark.{MLSQLSparkUtils, SparkEnv}
import tech.mlsql.common.utils.base.TryTool
import tech.mlsql.common.utils.distribute.socket.server.{Request, Response, SocketServerInExecutor, SocketServerSerDer}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.net.NetTool
import tech.mlsql.common.utils.network.NetUtils

/**
 * 2019-08-21 WilliamZhu(allwefantasy@gmail.com)
 */
class DriverLogServer[T](taskContextRef: AtomicReference[T]) extends SocketServerInExecutor[T](taskContextRef, "driver-log-server-in-driver") with Logging {

  @volatile private var markClose: AtomicBoolean = new AtomicBoolean(false)
  val client = new DriverLogClient()

  override def close() = {
    // make sure we only close once
    if (markClose.compareAndSet(false, true)) {
      logInfo(s"Shutdown ${host}. This may caused by the task is killed.")
    }
  }

  override def handleConnection(socket: Socket): Unit = {
    socket.setKeepAlive(true)
    val dIn = new DataInputStream(socket.getInputStream)
    val dOut = new DataOutputStream(socket.getOutputStream)

    TryTool.tryOrElse {
      while (true) {
        client.readRequest(dIn) match {
          case SendLog(token, logLine) =>
            if (token != taskContextRef.get()) {
              logInfo(s"${socket} auth fail. token:${token}")
              socket.close()
            } else {
              logInfo(logLine)
            }

        }
      }
    } {
      TryTool.tryOrNull {
        socket.close()
      }
    }


  }

  override def host: String = {
    if (SparkEnv.get == null || MLSQLSparkUtils.rpcEnv().address == null) NetTool.localHostName()
    else MLSQLSparkUtils.rpcEnv().address.host
  }
}

class DriverLogClient extends SocketServerSerDer[LogRequest, LogResponse]

case class SendLog(token: String, logLine: String) extends Request[LogRequest] {
  override def wrap = LogRequest(sendLog = this)
}

case class StopSendLog() extends Request[LogRequest] {
  override def wrap = LogRequest(stopSendLog = this)
}

case class Ok() extends Response[LogResponse] {
  override def wrap = LogResponse(ok = this)
}

case class Negative() extends Response[LogResponse] {
  override def wrap = LogResponse(negative = this)
}

case class LogResponse(
                        ok: Ok = null,
                        negative: Negative = null
                      ) {
  def unwrap: Response[_] = {
    if (ok != null) ok
    else if (negative != null) negative
    else null
  }
}

case class LogRequest(sendLog: SendLog = null, stopSendLog: StopSendLog = null) {
  def unwrap: Request[_] = {
    if (sendLog != null) sendLog
    else if (stopSendLog != null) stopSendLog
    else null
  }
}


