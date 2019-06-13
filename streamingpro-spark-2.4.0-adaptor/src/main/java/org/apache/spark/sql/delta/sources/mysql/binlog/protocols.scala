package org.apache.spark.sql.delta.sources.mysql.binlog

import com.github.shyiko.mysql.binlog.event.Event
import org.apache.spark.sql.delta.util.JsonUtils


case class MySQLBinlogServer(host: String, port: Int, fileName: String)

case class ExecutorBinlogServer(host: String, port: Int, fileName: String)

case class RawBinlogEvent(event: Event, binlogFilename: String)

case class MySQLConnectionInfo(host: String, port: Int, userName: String, password: String)

case class TableInfoCacheKey(uuidPrefix: String, databaseName: String, tableName: String, tableId: Long)

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

case class OffsetResponse(currentBinlogFile: String, currentBinlogPosition: Long, currentOffset: Long) extends Response {
  override def wrap: BinlogSocketResponse = BinlogSocketResponse(offsetResponse = this)
}

case class DataResponse(data: List[String]) extends Response {
  override def wrap: BinlogSocketResponse = BinlogSocketResponse(dataResponse = this)
}

case class RequestData(currentBinlogFile: String, startOffset: Long, endOffset: Long) extends Request {
  override def wrap: BinlogSocketRequest = BinlogSocketRequest(requestData = this)
}

case class RequestOffset(currentBinlogFile: String) extends Request {
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
