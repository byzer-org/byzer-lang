package org.apache.spark.sql.delta.sources.mysql.binlog

import java.net.Socket

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.EventType._
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer
import com.github.shyiko.mysql.binlog.event.{Event, RotateEventData, UpdateRowsEventData}
import org.apache.spark.SparkEnv
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

/**
  * 2019-06-13 WilliamZhu(allwefantasy@gmail.com)
  */
class BinLogSocketServerInExecutor extends SocketServerInExecutor[Array[Byte]]("binlog-socket-server-in-executor") with BinLogSocketServerSerDer {

  private var connectThread: Thread = null

  private var currentBinlogFile: String = null
  private var currentBinlogPosition: Long = 4

  private val batches = new ListBuffer[RawBinlogEvent]

  private var currentOffset: LongOffset = LongOffset(-1L)
  private var lastOffsetCommitted: LongOffset = LongOffset(-1L)

  private val sparkEnv = SparkEnv.get


  def addRecord(event: Event, binLogFilename: String) = {
    currentOffset += 1
    batches += (RawBinlogEvent(event, binLogFilename))
  }

  private def _connectMySQL(connect: MySQLConnectionInfo) = {
    val client = new BinaryLogClient(connect.host, connect.port, connect.userName, connect.password)
    val eventDeserializer = new EventDeserializer()
    eventDeserializer.setCompatibilityMode(
      EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
      EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
    )
    client.setEventDeserializer(eventDeserializer)

    //for now, we only care insert/update/delete three kinds of event
    client.registerEventListener(new BinaryLogClient.EventListener() {
      def onEvent(event: Event): Unit = {
        val eventType = event.getHeader
        eventType match {
          case TABLE_MAP =>

          case PRE_GA_WRITE_ROWS =>
          case WRITE_ROWS =>
          case EXT_WRITE_ROWS =>
            //event.getData[WriteRowsEventData]
            addRecord(event, client.getBinlogFilename)


          case PRE_GA_UPDATE_ROWS =>
          case UPDATE_ROWS =>
          case EXT_UPDATE_ROWS =>
            event.getData[UpdateRowsEventData].getRows
            addRecord(event, client.getBinlogFilename)

          case PRE_GA_DELETE_ROWS =>
          case DELETE_ROWS =>
          case EXT_DELETE_ROWS =>
            addRecord(event, client.getBinlogFilename)

          case ROTATE =>
            val rotateEventData = event.getData[RotateEventData]()
            currentBinlogFile = rotateEventData.getBinlogFilename
            currentBinlogPosition = rotateEventData.getBinlogPosition
          case _ =>
        }
      }
    })

    client.connect()
  }

  def loadSchemaInfo(connectionInfo: MySQLConnectionInfo, table: TableInfoCacheKey): StructType = {
    val parameters = Map(
      "url" -> s"jdbc:mysql://${connectionInfo.host}:${connectionInfo.port}",
      "user" -> connectionInfo.userName,
      "password" -> connectionInfo.password,
      "dbtable" -> s"${table.databaseName}.${table.tableName}"
    )
    val jdbcOptions = new JDBCOptions(parameters)
    val schema = JDBCRDD.resolveTable(jdbcOptions)
    schema
  }

  def connectMySQL(connect: MySQLConnectionInfo) = {

    connectThread = new Thread(s"connect mysql($host, $port) ") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          _connectMySQL(connect)
        } catch {
          case e: Exception =>
            throw e
        }

      }
    }
    connectThread.start()
  }


  def handleConnection(socket: Socket): Array[Byte] = {
    socket.setKeepAlive(true)
    while (true) {
      readRequest(socket.getInputStream) match {
        case request: RequestOffset =>
          sendResponse(socket.getOutputStream, OffsetResponse(currentBinlogFile, currentBinlogPosition, currentOffset.offset))
        case request: RequestData =>
          sendResponse(socket.getOutputStream, DataResponse(List()))
      }
    }
    Array()
  }
}
