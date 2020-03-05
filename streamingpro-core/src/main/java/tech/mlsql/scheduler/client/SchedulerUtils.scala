package tech.mlsql.scheduler.client

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import tech.mlsql.datalake.DataLake

/**
 * 27/2/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object SchedulerUtils {
  val DELTA_FORMAT = "org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource"
  val SCHEDULER_DEPENDENCY_JOBS = "scheduler.dependency_jobs"
  val SCHEDULER_TIME_JOBS = "scheduler.time_jobs"
  val SCHEDULER_TIME_JOBS_STATUS = "scheduler.time_jobs_status"
  val SCHEDULER_LOG = "scheduler.log"

  def saveTable(spark: SparkSession, data: DataFrame, tableName: String, updateCol: Option[String], isDelete: Boolean) = {
    val dataLake = new DataLake(spark)
    require(dataLake.isEnable, "please set -streaming.datalake.path enable delta db mode")
    val finalPath = dataLake.identifyToPath(tableName)
    val writer = data.write.format(DELTA_FORMAT)
    if (updateCol.isDefined) {
      writer.option("idCols", updateCol.get)
      if (isDelete) {
        writer.option("operation", "delete")
      }

    }
    try {
      writer.mode(SaveMode.Append).save(finalPath)
    } catch {
      case e: Exception =>
    }

  }

  def tryReadTable(spark: SparkSession, table: String, empty: () => DataFrame) = {
    try {
      readTable(spark, table)
    } catch {
      case e: Exception =>
        empty()
    }
  }

  def readTable(spark: SparkSession, tableName: String) = {
    val dataLake = new DataLake(spark)
    require(dataLake.isEnable, "please set -streaming.datalake.path enable delta db mode")
    val finalPath = dataLake.identifyToPath(tableName)
    spark.read.format(DELTA_FORMAT).load(finalPath)
  }
}
