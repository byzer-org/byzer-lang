package org.apache.spark.sql.delta.sources.mysql.binlog

import org.apache.spark.sql.delta.util.JsonUtils


object BinlogOffset {
  def fromOffset(recordOffset: Long) = {
    val item = recordOffset.toString
    val len = item.length
    val fileId = item.substring(0, len - 13).toLong
    val pos = item.substring(len - 13).toLong
    BinlogOffset(fileId, pos)
  }

  def toFileName(prefix: String, fileId: Long) = {
    s"${prefix}.${"%06d".format(fileId)}"
  }

  def fromFileAndPos(filename: String, pos: Long) = {
    BinlogOffset(filename.split("\\.").last.toLong, pos)
  }
}

case class BinlogOffset(fileId: Long, pos: Long) {
  def offset = (fileId.toString + "%013d".format(pos)).toLong
}

case class MySQLBinlogServer(host: String, port: Int)

case class ExecutorBinlogServer(host: String, port: Int)

case class MySQLConnectionInfo(host: String, port: Int, userName: String, password: String,
                               binlogFileName: Option[String],
                               recordPos: Option[Long],
                               databaseNamePattern: Option[String] = None,
                               tableNamePattern: Option[String] = None
                              )

case class TableInfoCacheKey(databaseName: String, tableName: String, tableId: Long)

// protocols
sealed trait Request {
  def wrap: BinlogSocketRequest

  def json: String = JsonUtils.toJson(wrap)
}

sealed trait Response {
  def wrap: BinlogSocketResponse

  def json: String = JsonUtils.toJson(wrap)
}

case class BinlogSocketResponse(offsetResponse: OffsetResponse = null,
                                dataResponse: DataResponse = null,
                                queueSizeResponse: QueueSizeResponse = null
                               ) {
  def unwrap: Response = {
    if (offsetResponse != null) {
      offsetResponse
    } else if (dataResponse != null) {
      dataResponse
    } else if (queueSizeResponse != null) {
      queueSizeResponse
    } else {
      null
    }
  }
}

case class QueueSizeResponse(size: Long) extends Response {
  override def wrap: BinlogSocketResponse = BinlogSocketResponse(queueSizeResponse = this)
}

case class ReportBinlogSocketServerHostAndPort(host: String, port: Int) extends Request {
  override def wrap: BinlogSocketRequest = BinlogSocketRequest(reportBinlogSocketServerHostAndPort = this)
}

case class ShutdownBinlogServer() extends Request {
  override def wrap: BinlogSocketRequest = BinlogSocketRequest(shutdownBinlogServer = this)
}

case class OffsetResponse(currentOffset: Long) extends Response {
  override def wrap: BinlogSocketResponse = BinlogSocketResponse(offsetResponse = this)
}

case class DataResponse(data: List[String]) extends Response {
  override def wrap: BinlogSocketResponse = BinlogSocketResponse(dataResponse = this)
}

case class RequestData(startOffset: Long, endOffset: Long) extends Request {
  override def wrap: BinlogSocketRequest = BinlogSocketRequest(requestData = this)
}

case class RequestOffset() extends Request {
  override def wrap: BinlogSocketRequest = BinlogSocketRequest(requestOffset = this)
}

case class RequestQueueSize() extends Request {
  override def wrap: BinlogSocketRequest = BinlogSocketRequest(requestQueueSize = this)
}

case class BinlogSocketRequest(
                                requestData: RequestData = null,
                                requestOffset: RequestOffset = null,
                                requestQueueSize: RequestQueueSize = null,
                                reportBinlogSocketServerHostAndPort: ReportBinlogSocketServerHostAndPort = null,
                                shutdownBinlogServer: ShutdownBinlogServer = null
                              ) {
  def unwrap: Request = {
    if (requestData != null) {
      requestData
    } else if (requestOffset != null) {
      requestOffset
    } else if (requestQueueSize != null) {
      requestQueueSize
    } else if (reportBinlogSocketServerHostAndPort != null) {
      reportBinlogSocketServerHostAndPort
    } else if (shutdownBinlogServer != null) {
      shutdownBinlogServer
    } else {
      null
    }
  }
}
