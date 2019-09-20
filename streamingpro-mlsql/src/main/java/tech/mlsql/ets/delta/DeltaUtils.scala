package tech.mlsql.ets.delta

import org.apache.spark.sql.delta.DeltaLog


object DeltaUtils {
  def tableStat(deltaLog: DeltaLog) = {
    val s = deltaLog.snapshot
    TableStat(s.sizeInBytes, s.numOfFiles, s.numOfMetadata, s.numOfProtocol, s.numOfRemoves, s.numOfSetTransactions)
  }
}

case class TableStat(
                      sizeInBytes: Long,
                      numOfFiles: Long,
                      numOfMetadata: Long,
                      numOfProtocol: Long,
                      numOfRemoves: Long,
                      numOfSetTransactions: Long)
