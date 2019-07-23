package org.apache.spark.sql.delta.sources

import java.io._
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.sources.mysql.binlog._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}
import org.apache.spark.{SparkEnv, TaskContext}

/**
  * This Datasource is used to consume MySQL binlog. Not support MariaDB yet because the connector we are using is
  * lack of the ability.
  * If you want to use this to upsert delta table, please set MySQL binlog_row_image to full so we can get the complete
  * record after updating.
  */
class MLSQLBinLogDataSource extends StreamSourceProvider with DataSourceRegister {


  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    require(schema.isEmpty, "Kafka source has a fixed schema and cannot be set with a custom one")
    (shortName(), {
      StructType(Seq(StructField("value", StringType)))
    })
  }

  /**
    * First, we will launch a task to
    *    1. start binlog client and setup a queue (where we put the binlog event)
    *    2. start a new socket the the executor where the task runs on, and return the connection message.
    * Second, Launch the MLSQLBinLogSource to consume the events:
    *    3. MLSQLBinLogSource get the host/port message and connect it to fetch the data.
    *    4. For now ,we will not support continue streaming.
    */
  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {

    val spark = sqlContext.sparkSession
    val logPrefix = parameters.getOrElse("logPrefix", "")
    val bingLogHost = parameters("host")
    val bingLogPort = parameters("port").toInt
    val bingLogUserName = parameters("userName")
    val bingLogPassword = parameters("password")
    val bingLogNamePrefix = parameters.get("bingLogNamePrefix")

    val databaseNamePattern = parameters.get("databaseNamePattern")
    val tableNamePattern = parameters.get("tableNamePattern")

    val startingOffsets = parameters.get("startingOffsets").map(f => LongOffset(f.toLong))

    startingOffsets match {
      case Some(value) =>
        assert(value.offset.toString.length >= 14, "The startingOffsets is combined at least 14 numbers. " +
          "The first six numbers are fileId, the left thirteen numbers are file line number.")
      case None =>
    }

    assert(startingOffsets.isDefined == bingLogNamePrefix.isDefined,
      "startingOffsets and bingLogNamePrefix should exists together ")

    val startOffsetInFile = startingOffsets.map(f => BinlogOffset.fromOffset(f.offset))
    val binlogFilename = startOffsetInFile.map(f => BinlogOffset.toFileName(bingLogNamePrefix.get, f.fileId))
    val binlogPos = startOffsetInFile.map(_.pos)

    val executorBinlogServerInfoRef = new AtomicReference[ReportBinlogSocketServerHostAndPort]()
    val tempSocketServerInDriver = new TempSocketServerInDriver(executorBinlogServerInfoRef)

    val tempSocketServerHost = tempSocketServerInDriver.host
    val tempSocketServerPort = tempSocketServerInDriver.port

    // We will buffer the binlog in Executor, if the buffer is greater then maxBinlogQueueSize, just
    // pause the MySQL binlog consumer, and stop put more data into buffer.
    // When the buffer is maxBinlogQueueSize/2 ,resume the binlog consumer
    val maxBinlogQueueSize = parameters.getOrElse("maxBinlogQueueSize", "500000").toLong

    val binlogServerId = UUID.randomUUID().toString

    def launchBinlogServer = {
      spark.sparkContext.setJobGroup(binlogServerId, s"binlog server (${bingLogHost}:${bingLogPort})", true)
      spark.sparkContext.parallelize(Seq("launch-binlog-socket-server")).map { item =>

        val taskContextRef: AtomicReference[TaskContext] = new AtomicReference[TaskContext]()
        taskContextRef.set(TaskContext.get())

        val executorBinlogServer = new BinLogSocketServerInExecutor(taskContextRef)
        executorBinlogServer.setMaxBinlogQueueSize(maxBinlogQueueSize)

        def sendStopBinlogServerRequest = {
          // send signal to stop server
          val socket2 = new Socket(executorBinlogServer.host, executorBinlogServer.port)
          val dout2 = new DataOutputStream(socket2.getOutputStream)
          BinLogSocketServerCommand.sendRequest(dout2,
            ShutdownBinlogServer())
          socket2.close()
        }

        TaskContext.get().addTaskFailureListener(new TaskFailureListener {
          override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
            taskContextRef.set(null)
            sendStopBinlogServerRequest

          }
        })

        TaskContext.get().addTaskCompletionListener(new TaskCompletionListener {
          override def onTaskCompletion(context: TaskContext): Unit = {
            taskContextRef.set(null)
            sendStopBinlogServerRequest
          }
        })


        val socket = new Socket(tempSocketServerHost, tempSocketServerPort)
        val dout = new DataOutputStream(socket.getOutputStream)
        BinLogSocketServerCommand.sendRequest(dout,
          ReportBinlogSocketServerHostAndPort(executorBinlogServer.host, executorBinlogServer.port))
        socket.close()

        SocketServerInExecutor.addNewBinlogServer(
          MySQLBinlogServer(bingLogHost, bingLogPort),
          executorBinlogServer)

        executorBinlogServer.connectMySQL(MySQLConnectionInfo(
          bingLogHost, bingLogPort,
          bingLogUserName, bingLogPassword,
          binlogFilename, binlogPos,
          databaseNamePattern, tableNamePattern), async = true)

        while (!TaskContext.get().isInterrupted() && !executorBinlogServer.isClosed) {
          Thread.sleep(1000)
        }

        ExecutorBinlogServer(executorBinlogServer.host, executorBinlogServer.port)
      }.collect()
    }

    new Thread("launch-binlog-socket-server-in-spark-job") {
      setDaemon(true)

      override def run(): Unit = {
        launchBinlogServer
      }
    }.start()

    var count = 60
    var executorBinlogServer: ExecutorBinlogServer = null
    while (executorBinlogServerInfoRef.get() == null) {
      Thread.sleep(1000)
      count -= 1
    }
    if (executorBinlogServerInfoRef.get() == null) {
      throw new RuntimeException("start BinLogSocketServerInExecutor fail")
    }
    val report = executorBinlogServerInfoRef.get()
    executorBinlogServer = ExecutorBinlogServer(report.host, report.port)
    MLSQLBinLogSource(executorBinlogServer, sqlContext.sparkSession, metadataPath, startingOffsets, parameters ++ Map("binlogServerId" -> binlogServerId))
  }

  override def shortName(): String = "mysql-binglog"
}

/**
  * This implementation will not work in production. We should do more thing on
  * something like fault recovery.
  *
  * @param executorBinlogServer
  * @param spark
  * @param parameters
  */
case class MLSQLBinLogSource(executorBinlogServer: ExecutorBinlogServer,
                             spark: SparkSession,
                             metadataPath: String,
                             startingOffsets: Option[LongOffset],
                             parameters: Map[String, String]
                            ) extends Source with BinLogSocketServerSerDer with Logging {


  private val VERSION = 1
  private val initialized: AtomicBoolean = new AtomicBoolean(false)

  private var socket: Socket = null
  private var dIn: DataInputStream = null
  private var dOut: DataOutputStream = null

  private val sparkEnv = SparkEnv.get

  private var currentPartitionOffsets: Option[LongOffset] = None

  private def initialize(): Unit = synchronized {
    socket = new Socket(executorBinlogServer.host, executorBinlogServer.port)
    dIn = new DataInputStream(socket.getInputStream)
    dOut = new DataOutputStream(socket.getOutputStream)
  }

  override def schema: StructType = {
    StructType(Seq(StructField("value", StringType)))
  }

  def request(req: Request) = {
    sendRequest(dOut, req)
    readResponse(dIn)
  }

  private lazy val initialPartitionOffsets = {
    val sqlContext = spark.sqlContext
    val offsetMetadataPath = metadataPath + "/binlog-offsets"
    val metadataLog = new HDFSMetadataLog[LongOffset](sqlContext.sparkSession, offsetMetadataPath) {
      override def serialize(metadata: LongOffset, out: OutputStream): Unit = {
        out.write(0) // A zero byte is written to support Spark 2.1.0 (SPARK-19517)
        val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
        writer.write("v" + VERSION + "\n")
        writer.write(metadata.json)
        writer.flush
      }

      override def deserialize(in: InputStream): LongOffset = {
        in.read() // A zero byte is read to support Spark 2.1.0 (SPARK-19517)
        val content = IOUtils.toString(new InputStreamReader(in, StandardCharsets.UTF_8))
        // HDFSMetadataLog guarantees that it never creates a partial file.
        assert(content.length != 0)
        if (content(0) == 'v') {
          val indexOfNewLine = content.indexOf("\n")
          if (indexOfNewLine > 0) {
            val version = parseVersion(content.substring(0, indexOfNewLine), VERSION)
            LongOffset(SerializedOffset(content.substring(indexOfNewLine + 1)))
          } else {
            throw new IllegalStateException(
              s"Log file was malformed: failed to detect the log file version line.")
          }
        } else {
          // The log was generated by Spark 2.1.0
          LongOffset(SerializedOffset(content))
        }
      }
    }

    metadataLog.get(0).getOrElse {
      val offsets = startingOffsets match {
        case Some(offset) => offset
        case None => getLatestOffset
      }
      metadataLog.add(0, offsets)
      logInfo(s"Initial offsets: $offsets")
      offsets
    }

  }

  def getLatestOffset = {
    sendRequest(dOut, RequestOffset())
    val response = readResponse(dIn).asInstanceOf[OffsetResponse]
    LongOffset(response.currentOffset)
  }

  override def getOffset: Option[Offset] = {
    synchronized {
      if (initialized.compareAndSet(false, true)) {
        initialize()
      }
    }
    val latest = getLatestOffset
    currentPartitionOffsets = Some(latest)
    Some(latest)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    synchronized {
      if (initialized.compareAndSet(false, true)) {
        initialize()
      }
    }

    initialPartitionOffsets

    val untilPartitionOffsets = LongOffset.convert(end)

    // On recovery, getBatch will get called before getOffset
    if (currentPartitionOffsets.isEmpty) {
      currentPartitionOffsets = untilPartitionOffsets
    }

    if (start.isDefined && start.get == end) {
      return spark.sqlContext.internalCreateDataFrame(
        spark.sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), schema, isStreaming = true)
    }

    // in case that we restore from the recovery, then we lose the start.
    // People can specify the startingOffset manually.
    val fromPartitionOffsets = start match {
      case Some(prevBatchEndOffset) =>
        LongOffset.convert(prevBatchEndOffset)
      case None =>
        Some(initialPartitionOffsets)
    }

    val executorBinlogServerCopy = executorBinlogServer.copy()

    val rdd = spark.sparkContext.parallelize(Seq("fetch-bing-log")).mapPartitions { iter =>
      val consumer = ExecutorBinlogServerConsumerCache.acquire(executorBinlogServerCopy)
      consumer.fetchData(fromPartitionOffsets.get, untilPartitionOffsets.get).toIterator
    }.map { cr =>
      InternalRow(UTF8String.fromString(cr))
    }
    spark.sqlContext.internalCreateDataFrame(rdd.setName("mysql-bin-log"), schema, isStreaming = true)
  }

  override def stop(): Unit = {
    // when the structure streaming is stopped(caused by  exception or manually killed),
    // we should make sure the binlog server is also killed.
    // here we use the binlogServerId as the spark job group id, and so we can cancel it.
    // Also notice that we should close the socket which we use to fetch the offset.
    try {
      spark.sparkContext.cancelJobGroup(parameters("binlogServerId"))
      socket.close()
    } catch {
      case e: Exception =>
        logError("", e)
    }

  }
}


case class ExecutorInternalBinlogConsumer(executorBinlogServer: ExecutorBinlogServer) extends BinLogSocketServerSerDer {
  val socket = new Socket(executorBinlogServer.host, executorBinlogServer.port)
  val dIn = new DataInputStream(socket.getInputStream)
  val dOut = new DataOutputStream(socket.getOutputStream)
  @volatile var inUse = true
  @volatile var markedForClose = false

  def fetchData(start: LongOffset, end: LongOffset) = {
    try {
      sendRequest(dOut, RequestData(
        start.offset,
        end.offset))
      val response = readResponse(dIn)
      response.asInstanceOf[DataResponse].data
    } finally {
      ExecutorBinlogServerConsumerCache.release(this)
    }
  }

  def close = {
    socket.close()
  }
}










