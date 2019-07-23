package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 2019-06-08 WilliamZhu(allwefantasy@gmail.com)
  */
case class CompactTableInDelta(
                                deltaLog: DeltaLog,
                                options: DeltaOptions,
                                partitionColumns: Seq[String],
                                configuration: Map[String, String]
                              ) {
  def run(sparkSession: SparkSession): Seq[Row] = {
    Seq[Row]()
  }
}

object CompactTableInDelta {
  val COMPACT_VERSION_OPTION = "compactVersion"
  val COMPACT_NUM_FILE_PER_DIR = "compactNumFilePerDir"
  val COMPACT_RETRY_TIMES_FOR_LOCK = "compactRetryTimesForLock"
}
