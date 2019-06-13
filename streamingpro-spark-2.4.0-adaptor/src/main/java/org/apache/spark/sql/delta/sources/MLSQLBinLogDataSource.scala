package org.apache.spark.sql.delta.sources

import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.sources.mysql.binlog._
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * This Datasource is used to consume MySQL binlog. Not support MariaDB yet because the connector we are using is
  * lack of the ability.
  * If you want to use this to upsert delta table, please set MySQL binlog_row_image to full so we can get the complete
  * record after updating.
  */
class MLSQLBinLogDataSource extends StreamSourceProvider with DataSourceRegister {
  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String, parameters: Map[String, String]): (String, StructType) = ???

  /**
    * First, we will launch a task to
    *    1. start binglog client and setup a queue (where we put the binlog event)
    *    2. start a new socket the the executor where the task runs on, and return the connection message.
    * Second, Launch the MLSQLBinLogSource to consume the events:
    *    3. MLSQLBinLogSource get the host/port message and connect it to fetch the data.
    *    4. For now ,we will not support continue streaming.
    */
  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {

    val spark = sqlContext.sparkSession

    val bingLogHost = parameters("bingLogHost")
    val bingLogPort = parameters("bingLogPort").toInt
    val bingLogName = parameters.getOrElse("bingLogName", BinLogSocketServerInExecutor.FILE_NAME_NOT_SET)

    val executorBinlogServer = spark.sparkContext.parallelize(Seq("launch-binlog-socket-server")).map { item =>
      val executorBinlogServer = new BinLogSocketServerInExecutor()
      SocketServerInExecutor.addNewBinlogServer(MySQLBinlogServer(bingLogHost, bingLogPort, bingLogName), executorBinlogServer)
      ExecutorBinlogServer(executorBinlogServer.host, executorBinlogServer.port, bingLogName)
    }.collect().head

    MLSQLBinLogSource(executorBinlogServer, sqlContext.sparkSession, Map())
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
                             spark: SparkSession, parameters: Map[String, String]
                            ) extends Source with Logging {

  private val initialized: AtomicBoolean = new AtomicBoolean(false)

  private var socket: Socket = null

  private val sparkEnv = SparkEnv.get

  private def initialize(): Unit = synchronized {
    socket = new Socket(executorBinlogServer.host, executorBinlogServer.port)
  }

  override def schema: StructType = {
    StructType(Seq(StructField("value", StringType)))
  }

  override def getOffset: Option[Offset] = {
    synchronized {
      if (initialized.compareAndSet(false, true)) {
        initialize()
      }
    }
    Option(LongOffset(0))
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val rdd = spark.sparkContext.parallelize(Seq("fetch-bing-log")).mapPartitions { iter =>
      val consumer = ExecutorBinlogServerConsumerCache.acquire(executorBinlogServer)
      consumer.fetchData(start.get, end).toIterator
    }.map { cr =>
      InternalRow(cr)
    }
    spark.sqlContext.internalCreateDataFrame(rdd.setName("mysql-bin-log"), schema, isStreaming = true)
  }

  override def stop(): Unit = {
    socket.close()
  }
}


case class ExecutorInternalBinlogConsumer(executorBinlogServer: ExecutorBinlogServer) extends BinLogSocketServerSerDer {
  val socket = new Socket(executorBinlogServer.host, executorBinlogServer.port)
  @volatile var inUse = true
  @volatile var markedForClose = false

  def fetchData(start: Offset, end: Offset) = {
    sendRequest(socket.getOutputStream, RequestData(
      executorBinlogServer.fileName,
      start.asInstanceOf[LongOffset].offset,
      end.asInstanceOf[LongOffset].offset))
    val response = readResponse(socket.getInputStream)
    response.asInstanceOf[DataResponse].data
  }

  def close = {
    socket.close()
  }
}










