package tech.mlsql.store

import org.apache.spark.sql.{DataFrame, SparkSession}
import tech.mlsql.store.DictType.DictType

/**
 * 16/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait DBStore {
  def readTable(spark: SparkSession, tableName: String): DataFrame

  def tryReadTable(spark: SparkSession, table: String, empty: () => DataFrame): DataFrame

  def saveTable(spark: SparkSession, data: DataFrame, tableName: String, updateCol: Option[String], isDelete: Boolean)

  def saveConfig(spark: SparkSession,appPrefix: String, name: String, value: String, dictType: DictType): Unit

  def readConfig(spark: SparkSession,appPrefix: String, name: String, dictType: DictType): Option[WDictStore]

  def readAllConfig(spark: SparkSession,appPrefix: String): List[WDictStore]
}

object DBStore {
  private var _store: DBStore = new DeltaLakeDBStore

  def store = {
    assert(_store != null, "DBStore is not initialed")
    _store
  }

  def set(store: DBStore) = _store = store
}

case class WDictStore(id: Int, var name: String, var value: String, var dictType: Int)

object DictType extends Enumeration {
  type DictType = Value
  val DB = Value(0)
  val APP_CONFIG = Value(1)
  // 实例（一组）到数据库
  val APP_TO_DB = Value(2)
  // 实例（一组）到另外一组实例代理
  val APP_TO_APP_PROXY = Value(3)
  val MLSQL_CONFIG = Value(4)
}
