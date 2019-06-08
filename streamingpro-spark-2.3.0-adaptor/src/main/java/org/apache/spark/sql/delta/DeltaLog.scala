package org.apache.spark.sql.delta

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.internal.SQLConf

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

  def getHistory(num: Option[Int]) = {
    Seq[CommitInfo]()
  }
}

class DeltaOptions(
                    @transient protected val options: CaseInsensitiveMap[String],
                    @transient protected val sqlConf: SQLConf) {

  def this(options: Map[String, String], conf: SQLConf) = this(CaseInsensitiveMap(options), conf)
}


