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

case class MySQLBinlogServer(host: String, port: Int, fileName: Option[String])

case class ExecutorBinlogServer(host: String, port: Int, fileName: Option[String])

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
                                dataResponse: DataResponse = null) {
  def unwrap: Response = {
    if (offsetResponse != null) {
      offsetResponse
    } else if (dataResponse != null) {
      dataResponse
    } else {
      null
    }
  }
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

case class BinlogSocketRequest(
                                requestData: RequestData = null,
                                requestOffset: RequestOffset = null) {
  def unwrap: Request = {
    if (requestData != null) {
      requestData
    } else if (requestOffset != null) {
      requestOffset
    } else {
      null
    }
  }
}
