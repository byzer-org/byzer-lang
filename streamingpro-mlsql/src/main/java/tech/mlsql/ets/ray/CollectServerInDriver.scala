package tech.mlsql.ets.ray

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import org.apache.spark.{MLSQLSparkUtils, SparkEnv}
import tech.mlsql.common.utils.distribute.socket.server.{ReportHostAndPort, ReportSingleAction, SocketServerInExecutor, SocketServerSerDer}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.network.NetUtils

import scala.collection.mutable.ArrayBuffer

class CollectServerInDriver[T](context: AtomicReference[ArrayBuffer[ReportHostAndPort]], taskContextRef: AtomicReference[T])
  extends SocketServerInExecutor[T](taskContextRef, "CollectServerInDriver") with Logging {

  @volatile private var markClose: AtomicBoolean = new AtomicBoolean(false)
  val client = new SocketServerSerDer[ReportSingleAction, ReportSingleAction]() {}

  override def host: String = {
    if (SparkEnv.get == null) {
      //When SparkEnv.get is null, the program may run in a test
      //So return local address would be ok.
      "127.0.0.1"
    } else {
      if (MLSQLSparkUtils.rpcEnv().address == null) NetUtils.getHost
      else MLSQLSparkUtils.rpcEnv().address.host
    }
  }

  override def close() = {
    // make sure we only close once
    if (markClose.compareAndSet(false, true)) {
      logInfo(s"Shutdown ${host}. This may caused by the task is killed.")
    }
  }

  def shutdown = {
    taskContextRef.set(null.asInstanceOf[T])
    _server.close()
  }

  def handleConnection(socket: Socket): Unit = {
    val dIn = new DataInputStream(socket.getInputStream)
    val dOut = new DataOutputStream(socket.getOutputStream)
    client.readRequest(dIn) match {
      case rha: ReportHostAndPort =>
        println(rha)
        context.get() += rha
        client.sendRequest(dOut, rha)
    }
  }


}
