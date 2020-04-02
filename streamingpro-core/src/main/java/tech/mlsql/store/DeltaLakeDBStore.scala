package tech.mlsql.store

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import tech.mlsql.datalake.DataLake
import tech.mlsql.scheduler.client.SchedulerUtils.DELTA_FORMAT
import tech.mlsql.store.DictType.DictType

/**
 * 16/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class DeltaLakeDBStore extends DBStore {
  private val configTableName = "__mlsql__.wDictStore"

  override def saveConfig(spark: SparkSession, appPrefix: String, name: String, value: String, dictType: DictType): Unit = {
    val key = s"${appPrefix}_${name}"
    import spark.implicits._
    saveTable(spark, spark.createDataset[WDictStore](Seq(WDictStore(0, key, value, dictType.id))).toDF(), configTableName, Option(key), false)
  }

  override def readConfig(spark: SparkSession, appPrefix: String, name: String, dictType: DictType): Option[WDictStore] = {
    import spark.implicits._
    val key = s"${appPrefix}_${name}"
    val emptyTable = spark.createDataset[WDictStore](Seq()).toDF()
    tryReadTable(spark, configTableName, () => emptyTable).as[WDictStore].filter(item => item.name == key && item.dictType == dictType.id).collect().headOption
  }


  override def readAllConfig(spark: SparkSession, appPrefix: String): List[WDictStore] = {
    import spark.implicits._
    val emptyTable = spark.createDataset[WDictStore](Seq()).toDF()
    tryReadTable(spark, configTableName, () => emptyTable).as[WDictStore].filter(item => item.name.startsWith(appPrefix)).collect().toList
  }

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
