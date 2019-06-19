package org.apache.spark.sql.delta.sources.mysql.binlog

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.regex.Pattern

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.EventType._
import com.github.shyiko.mysql.binlog.event._
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.sources.mysql.binlog.io.{DeleteRowsWriter, EventInfo, InsertRowsWriter, UpdateRowsWriter}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * 2019-06-13 WilliamZhu(allwefantasy@gmail.com)
  */
class BinLogSocketServerInExecutor[T](taskContextRef: AtomicReference[T])
  extends SocketServerInExecutor[T](taskContextRef, "binlog-socket-server-in-executor")
    with BinLogSocketServerSerDer with Logging {

  private var connectThread: Thread = null

  private var binaryLogClient: BinaryLogClient = null

  private var currentBinlogFile: String = null

  private var currentBinlogPosition: Long = 4

  private val queue = new LinkedBlockingQueue[RawBinlogEvent]()

  private var databaseNamePattern: Option[Pattern] = None
  private var tableNamePattern: Option[Pattern] = None

  private var maxBinlogQueueSize: Long = 0l

  private var connect: MySQLConnectionInfo = null

  @volatile private var skipTable = false
  @volatile private var currentTable: TableInfo = _
  @volatile private var markClose: AtomicBoolean = new AtomicBoolean(false)
  @volatile private var markPause: AtomicBoolean = new AtomicBoolean(false)

  private val connections = new ArrayBuffer[Socket]()

  private val currentQueueSize = new AtomicLong(0)

  val updateRowsWriter = new UpdateRowsWriter()
  val deleteRowsWriter = new DeleteRowsWriter()
  val insertRowsWriter = new InsertRowsWriter()

  def isClosed = {
    markClose.get()
  }

  def setMaxBinlogQueueSize(value: Long) = {
    this.maxBinlogQueueSize = value
  }

  private val tableInfoCache = new ConcurrentHashMap[TableInfoCacheKey, TableInfo]()

  def assertTable = {
    if (currentTable == null) {
      throw new RuntimeException("No table information is available for this event, cannot process further.")
    }
  }

  def addRecord(event: Event, binLogFilename: String, eventType: String) = {
    assertTable
    if (currentQueueSize.get() > maxBinlogQueueSize && !markPause.get()) {
      pause
    } else if (currentQueueSize.get() < maxBinlogQueueSize / 2 && markPause.get()) {
      resume
    }
    currentQueueSize.incrementAndGet()
    queue.offer(new RawBinlogEvent(event, currentTable, binLogFilename, eventType, currentBinlogPosition))
  }

  private def _connectMySQL(connect: MySQLConnectionInfo) = {
    binaryLogClient = new BinaryLogClient(connect.host, connect.port, connect.userName, connect.password)

    connect.binlogFileName match {
      case Some(filename) =>
        binaryLogClient.setBinlogFilename(filename)
        currentBinlogFile = filename
      case _ =>
    }

    connect.recordPos match {
      case Some(recordPos) => binaryLogClient.setBinlogPosition(recordPos)
      case _ =>
    }

    val eventDeserializer = new EventDeserializer()
    eventDeserializer.setCompatibilityMode(
      EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
      EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
    )
    binaryLogClient.setEventDeserializer(eventDeserializer)



    //for now, we only care insert/update/delete three kinds of event
    binaryLogClient.registerEventListener(new BinaryLogClient.EventListener() {
      def onEvent(event: Event): Unit = {
        val header = event.getHeader[EventHeaderV4]()
        val eventType = header.getEventType
        if (eventType != ROTATE && eventType != FORMAT_DESCRIPTION) {
          currentBinlogPosition = header.getPosition
        }

        eventType match {
          case TABLE_MAP =>
            val data = event.getData[TableMapEventData]()
            skipTable = !(databaseNamePattern
              .map(_.matcher(data.getDatabase).matches())
              .getOrElse(false) && tableNamePattern
              .map(_.matcher(data.getTable).matches())
              .getOrElse(false))

            if (!skipTable) {
              val cacheKey = new TableInfoCacheKey(data.getDatabase, data.getTable, data.getTableId)
              currentTable = tableInfoCache.get(cacheKey)
              if (currentTable == null) {
                val tableSchemaInfo = loadSchemaInfo(connect, cacheKey)
                val currentTableRef = new TableInfo(cacheKey.databaseName, cacheKey.tableName, cacheKey.tableId, tableSchemaInfo.json)
                tableInfoCache.put(cacheKey, currentTableRef)
                currentTable = currentTableRef
              }

            } else currentTable = null

          case PRE_GA_WRITE_ROWS =>
          case WRITE_ROWS =>
          case EXT_WRITE_ROWS =>
            if (!skipTable) addRecord(event, binaryLogClient.getBinlogFilename, EventInfo.INSERT_EVENT)


          case PRE_GA_UPDATE_ROWS =>
          case UPDATE_ROWS =>
          case EXT_UPDATE_ROWS =>
            if (!skipTable) {
              addRecord(event, binaryLogClient.getBinlogFilename, EventInfo.UPDATE_EVENT)
            }


          case PRE_GA_DELETE_ROWS =>
          case DELETE_ROWS =>
          case EXT_DELETE_ROWS =>
            if (!skipTable) {
              addRecord(event, binaryLogClient.getBinlogFilename, EventInfo.DELETE_EVENT)
            }


          case ROTATE =>
            val rotateEventData = event.getData[RotateEventData]()
            currentBinlogFile = rotateEventData.getBinlogFilename
            currentBinlogPosition = rotateEventData.getBinlogPosition
          case _ =>
        }

      }
    })

    binaryLogClient.connect()
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

  def connectMySQL(_connect: MySQLConnectionInfo, async: Boolean = true) = {
    connect = _connect
    databaseNamePattern = connect.databaseNamePattern.map(Pattern.compile)

    tableNamePattern = connect.tableNamePattern.map(Pattern.compile)
    if (async) {
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
    } else {
      _connectMySQL(connect)
    }

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

    val jsonList = try {
      writer.writeEvent(rawBinlogEvent)
    } catch {
      case e: Exception =>
        logError("", e)
        new util.ArrayList[String]()
    }
    jsonList
  }

  def tryWithoutException(fun: () => Unit) = {
    try {
      fun()
    } catch {
      case e: Exception =>
    }
  }

  def pause = {
    if (markPause.compareAndSet(false, true)) {
      if (binaryLogClient != null) {
        tryWithoutException(() => {
          binaryLogClient.disconnect()
        })
      }
    }
  }

  def resume = {
    if (markPause.compareAndSet(true, false)) {
      connectThread = new Thread(s"connect mysql(${connect.host}, ${connect.port}) ") {
        setDaemon(true)

        override def run(): Unit = {
          try {
            // todo: should be able to reattempt, because when MySQL is busy, it's hard to connect
            binaryLogClient.connect()
          } catch {
            case e: Exception =>
              throw e
          }

        }
      }
      connectThread.start()
    }
  }

  override def close() = {
    // make sure we only close once
    if (markClose.compareAndSet(false, true)) {
      logInfo(s"Shutdown ${server}. This may caused by the task is killed.")

      connections.foreach { socket =>
        tryWithoutException(() => {
          socket.close()
        })
      }
      connections.clear()

      if (binaryLogClient != null) {
        tryWithoutException(() => {
          binaryLogClient.disconnect()
        })
      }

      if (server != null) {
        tryWithoutException(() => {
          server.close()
        })
      }

      tryWithoutException(() => {
        queue.clear()
      })


    }
  }

  def handleConnection(socket: Socket): Unit = {
    connections += socket
    socket.setKeepAlive(true)
    val dIn = new DataInputStream(socket.getInputStream)
    val dOut = new DataOutputStream(socket.getOutputStream)

    while (true) {
      readRequest(dIn) match {
        case _: ShutdownBinlogServer =>
          close()
        case _: RequestQueueSize => {
          sendResponse(dOut, QueueSizeResponse(queue.size()))
        }
        case _: RequestOffset =>
          sendResponse(dOut, OffsetResponse(BinlogOffset.fromFileAndPos(currentBinlogFile, currentBinlogPosition + 1).offset))
        case request: RequestData =>
          val start = request.startOffset
          val end = request.endOffset

          // this is used to get all data in queue.
          // normally for test
          if (start == -1 && end == -1) {
            val res = ArrayBuffer[String]()
            val iter = queue.iterator()

            while (iter.hasNext) {
              res ++= convertRawBinlogEventRecord(iter.next()).asScala
            }
            sendResponse(dOut, DataResponse(res.toList))
            return
          }

          var item: RawBinlogEvent = queue.poll()
          val res = ArrayBuffer[String]()
          try {
            while (item != null && toOffset(item) >= start && toOffset(item) < end) {
              res ++= convertRawBinlogEventRecord(item).asScala
              item = queue.poll()
              currentQueueSize.decrementAndGet()
            }
          } catch {
            case e: Exception =>
              logError("", e)
          }
          sendResponse(dOut, DataResponse(res.toList))
      }
    }

  }
}

object BinLogSocketServerInExecutor {
  val FILE_NAME_NOT_SET = "file_name_not_set"
}
