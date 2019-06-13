package org.apache.spark.sql.delta.sources.mysql.binlog

import java.net.Socket
import java.util.concurrent.LinkedBlockingQueue

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.EventType._
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer
import com.github.shyiko.mysql.binlog.event.{Event, EventHeaderV4, RotateEventData}
import org.apache.spark.sql.delta.sources.mysql.binlog.io.EventInfo
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-06-13 WilliamZhu(allwefantasy@gmail.com)
  */
class BinLogSocketServerInExecutor extends SocketServerInExecutor[Array[Byte]]("binlog-socket-server-in-executor") with BinLogSocketServerSerDer {

  private var connectThread: Thread = null

  private var currentBinlogFile: String = null

  private var currentBinlogPosition: Long = 4

  private val queue = new LinkedBlockingQueue[RawBinlogEvent]()


  def addRecord(event: Event, binLogFilename: String, eventType: String) = {
    queue.offer(new RawBinlogEvent(event, binLogFilename, eventType, currentBinlogPosition))
  }

  private def _connectMySQL(connect: MySQLConnectionInfo) = {
    val client = new BinaryLogClient(connect.host, connect.port, connect.userName, connect.password)

    connect.binlogFileName match {
      case Some(filename) => client.setBinlogFilename(filename)
      case _ =>
    }

    connect.recordPos match {
      case Some(recordPos) => client.setBinlogPosition(recordPos)
      case _ =>
    }

    val eventDeserializer = new EventDeserializer()
    eventDeserializer.setCompatibilityMode(
      EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
      EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
    )
    client.setEventDeserializer(eventDeserializer)

    //for now, we only care insert/update/delete three kinds of event
    client.registerEventListener(new BinaryLogClient.EventListener() {
      def onEvent(event: Event): Unit = {
        val header = event.getHeader[EventHeaderV4]()
        val eventType = header.getEventType
        if (eventType != ROTATE && eventType != FORMAT_DESCRIPTION) {
          currentBinlogPosition = header.getPosition
        }

        eventType match {
          case TABLE_MAP =>

          case PRE_GA_WRITE_ROWS =>
          case WRITE_ROWS =>
          case EXT_WRITE_ROWS =>
            addRecord(event, client.getBinlogFilename, EventInfo.INSERT_EVENT)


          case PRE_GA_UPDATE_ROWS =>
          case UPDATE_ROWS =>
          case EXT_UPDATE_ROWS =>
            addRecord(event, client.getBinlogFilename, EventInfo.UPDATE_EVENT)

          case PRE_GA_DELETE_ROWS =>
          case DELETE_ROWS =>
          case EXT_DELETE_ROWS =>
            addRecord(event, client.getBinlogFilename, EventInfo.DELETE_EVENT)

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

  private def toOffset(rawBinlogEvent: RawBinlogEvent) = {
    BinlogOffset.fromFileAndPos(rawBinlogEvent.getBinlogFilename, rawBinlogEvent.getPos).offset
  }

  def handleConnection(socket: Socket): Array[Byte] = {
    socket.setKeepAlive(true)
    while (true) {
      readRequest(socket.getInputStream) match {
        case request: RequestOffset =>
          sendResponse(socket.getOutputStream, OffsetResponse(BinlogOffset.fromFileAndPos(currentBinlogFile, currentBinlogPosition + 1).offset))
        case request: RequestData =>
          val start = request.startOffset
          val end = request.endOffset
          var item: RawBinlogEvent = new RawBinlogEvent(null, "", 4)
          val res = ArrayBuffer[RawBinlogEvent]()
          while (item != null && toOffset(item) >= start && toOffset(item) < end) {
            item = queue.poll()
            if (item != null) {
              res += item
            }
          }
          sendResponse(socket.getOutputStream, DataResponse(List()))
      }
    }
    Array()
  }
}
