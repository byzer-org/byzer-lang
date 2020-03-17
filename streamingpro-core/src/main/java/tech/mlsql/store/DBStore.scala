package tech.mlsql.store

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 16/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait DBStore {
  def readTable(spark: SparkSession, tableName: String): DataFrame

  def tryReadTable(spark: SparkSession, table: String, empty: () => DataFrame): DataFrame

  def saveTable(spark: SparkSession, data: DataFrame, tableName: String, updateCol: Option[String], isDelete: Boolean)
}

object DBStore {
  private var _store: DBStore = new DeltaLakeDBStore

  def store = {
    assert(_store != null, "DBStore is not initialed")
    _store
  }

  def set(store: DBStore) = _store = store
}
