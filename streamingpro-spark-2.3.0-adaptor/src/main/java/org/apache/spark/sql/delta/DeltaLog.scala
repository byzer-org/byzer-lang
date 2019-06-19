package org.apache.spark.sql.delta

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.CommitInfo

/**
  * 2019-06-08 WilliamZhu(allwefantasy@gmail.com)
  * Only for compilation
  */
object DeltaLog {
  def forTable(spark: SparkSession, dataPath: String): DeltaLog = {
    new DeltaLog()
  }
}

class DeltaLog {
  def history: DeltaLog = {
    this
  }

  def snapshot = {
    Snapshot(-1)
  }

  def getHistory(num: Option[Int]) = {
    Seq[CommitInfo]()
  }
}

case class Snapshot(val version: Long)



