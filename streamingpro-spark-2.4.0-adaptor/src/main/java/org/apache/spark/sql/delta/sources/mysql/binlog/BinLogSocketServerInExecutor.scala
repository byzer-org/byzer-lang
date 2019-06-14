package org.apache.spark.sql.delta.sources.mysql.binlog

import java.net.Socket
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.regex.Pattern

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.EventType._
import com.github.shyiko.mysql.binlog.event._
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer
import org.apache.spark.sql.delta.sources.mysql.binlog.io.{DeleteRowsWriter, EventInfo, InsertRowsWriter, UpdateRowsWriter}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * 2019-06-13 WilliamZhu(allwefantasy@gmail.com)
  */
class BinLogSocketServerInExecutor
  extends SocketServerInExecutor[Array[Byte]]("binlog-socket-server-in-executor")
    with BinLogSocketServerSerDer {

  private var connectThread: Thread = null

  private var currentBinlogFile: String = null

  private var currentBinlogPosition: Long = 4

  private val queue = new LinkedBlockingQueue[RawBinlogEvent]()

  private var databaseNamePattern: Option[Pattern] = None
  private var tableNamePattern: Option[Pattern] = None
  @volatile private var skipTable = false
  @volatile private var currentTable: TableInfo = _

  val updateRowsWriter = new UpdateRowsWriter()
  val deleteRowsWriter = new DeleteRowsWriter()
  val insertRowsWriter = new InsertRowsWriter()


  private val tableInfoCache = new ConcurrentHashMap[TableInfoCacheKey, TableInfo]()

  def assertTable = {
    if (currentTable == null) {
      throw new RuntimeException("No table information is available for this event, cannot process further.")
    }
  }

  def addRecord(event: Event, binLogFilename: String, eventType: String) = {
    assertTable
    queue.offer(new RawBinlogEvent(event, currentTable, binLogFilename, eventType, currentBinlogPosition))
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
            val data = event.getData[TableMapEventData]()
            skipTable = databaseNamePattern
              .map(_.matcher(data.getDatabase).matches())
              .getOrElse(false) || tableNamePattern
              .map(_.matcher(data.getTable).matches())
              .getOrElse(false)

            if (!skipTable) {
              val cacheKey = new TableInfoCacheKey(data.getDatabase, data.getTable, data.getTableId)
              currentTable = tableInfoCache.get(cacheKey)
              if (currentTable == null) {
                val tableSchemaInfo = loadSchemaInfo(connect, cacheKey)
                val currentTableRef = new TableInfo(cacheKey.databaseName, cacheKey.tableName, cacheKey.tableId, tableSchemaInfo.json)
                tableInfoCache.put(cacheKey, currentTableRef)
              }

            } else currentTable = null

          case PRE_GA_WRITE_ROWS =>
          case WRITE_ROWS =>
          case EXT_WRITE_ROWS =>
            if (!skipTable) addRecord(event, client.getBinlogFilename, EventInfo.INSERT_EVENT)


          case PRE_GA_UPDATE_ROWS =>
          case UPDATE_ROWS =>
          case EXT_UPDATE_ROWS =>
            if (!skipTable) {
              addRecord(event, client.getBinlogFilename, EventInfo.UPDATE_EVENT)
            }


          case PRE_GA_DELETE_ROWS =>
          case DELETE_ROWS =>
          case EXT_DELETE_ROWS =>
            if (!skipTable) {
              addRecord(event, client.getBinlogFilename, EventInfo.DELETE_EVENT)
            }


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

    databaseNamePattern = connect.databaseNamePattern.map(Pattern.compile)

    tableNamePattern = connect.tableNamePattern.map(Pattern.compile)

    connectThread = new Thread(s"connect mysql(${connect.host}, ${connect.port}) ") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          // todo: should be able to reattempt, because when MySQL is busy, it's hard to connect
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

  def convertRawBinlogEventRecord(rawBinlogEvent: RawBinlogEvent) = {
    val writer = rawBinlogEvent.getEventType() match {
      case "insert" => insertRowsWriter
      case "update" => updateRowsWriter
      case "delete" => deleteRowsWriter
    }
    writer.writeEvent(rawBinlogEvent)
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
          var item: RawBinlogEvent = new RawBinlogEvent(null, null, "", null, 4)
          val res = ArrayBuffer[String]()
          while (item != null && toOffset(item) >= start && toOffset(item) < end) {
            item = queue.poll()
            if (item != null) {
              res ++= convertRawBinlogEventRecord(item).asScala
            }
          }
          sendResponse(socket.getOutputStream, DataResponse(res.toList))
      }
    }
    Array()
  }
}

object BinLogSocketServerInExecutor {
  val FILE_NAME_NOT_SET = "file_name_not_set"
}
