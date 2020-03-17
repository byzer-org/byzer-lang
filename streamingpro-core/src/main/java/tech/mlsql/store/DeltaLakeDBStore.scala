package tech.mlsql.store

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import tech.mlsql.datalake.DataLake
import tech.mlsql.scheduler.client.SchedulerUtils.DELTA_FORMAT

/**
 * 16/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class DeltaLakeDBStore extends DBStore {
  override def saveTable(spark: SparkSession, data: DataFrame, tableName: String, updateCol: Option[String], isDelete: Boolean) = {
    val dataLake = new DataLake(spark)
    require(dataLake.isEnable, "please set -streaming.datalake.path enable delta db mode")
    val finalPath = dataLake.identifyToPath(tableName)
    val writer = data.repartition(1).write.format(DELTA_FORMAT)
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

  override def tryReadTable(spark: SparkSession, table: String, empty: () => DataFrame) = {
    try {
      readTable(spark, table)
    } catch {
      case e: Exception =>
        empty()
    }
  }

  override def readTable(spark: SparkSession, tableName: String) = {
    val dataLake = new DataLake(spark)
    require(dataLake.isEnable, "please set -streaming.datalake.path enable delta db mode")
    val finalPath = dataLake.identifyToPath(tableName)
    spark.read.format(DELTA_FORMAT).load(finalPath)
  }
}
