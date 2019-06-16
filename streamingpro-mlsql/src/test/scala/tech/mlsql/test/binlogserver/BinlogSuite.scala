package tech.mlsql.test.binlogserver

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.delta.sources.mysql.binlog._
import org.apache.spark.sql.execution.streaming.LongOffset
import org.scalatest.FunSuite

/**
  * 2019-06-15 WilliamZhu(allwefantasy@gmail.com)
  */
class BinlogSuite extends FunSuite with BinLogSocketServerSerDer {
  test("socket server communicate") {

    val parameters = Map(
      "host" -> "127.0.0.1",
      "port" -> "3306",
      "userName" -> "root",
      "password" -> "mlsql"
    )

    val bingLogHost = parameters("host")
    val bingLogPort = parameters("port").toInt
    val bingLogUserName = parameters("userName")
    val bingLogPassword = parameters("password")
    val bingLogName = parameters.get("bingLogName")

    val databaseNamePattern = parameters.get("databaseNamePattern")
    val tableNamePattern = parameters.get("tableNamePattern")

    val metadataPath = parameters.getOrElse("metadataPath", "offsets")
    val startingOffsets = parameters.get("startingOffsets").map(f => LongOffset(f.toLong))
    val taskContextRef = new AtomicReference[Boolean]()
    taskContextRef.set(true)
    val executorBinlogServer = new BinLogSocketServerInExecutor(taskContextRef)

    executorBinlogServer.connectMySQL(MySQLConnectionInfo(
      bingLogHost, bingLogPort,
      bingLogUserName, bingLogPassword,
      Some("mysql-bin.000004"), Some(4),
      databaseNamePattern, tableNamePattern))

    Thread.sleep(10000)
    val executorServer = ExecutorBinlogServer(executorBinlogServer.host, executorBinlogServer.port)
    val socket = new Socket(executorServer.host, executorServer.port)
    val dout = new DataOutputStream(socket.getOutputStream)
    val din = new DataInputStream(socket.getInputStream)
    sendRequest(dout, RequestOffset())
    val offset1 = readResponse(din).asInstanceOf[OffsetResponse]
    sendRequest(dout, RequestData(40000000000004l, offset1.currentOffset))
    val res2 = readResponse(din).asInstanceOf[DataResponse]
    println(res2)
    socket.close()
  }
}
