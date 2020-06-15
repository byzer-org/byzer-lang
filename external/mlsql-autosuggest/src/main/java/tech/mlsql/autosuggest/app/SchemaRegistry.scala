package tech.mlsql.autosuggest.app

import com.alibaba.druid.util.JdbcConstants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import tech.mlsql.autosuggest.AutoSuggestContext
import tech.mlsql.autosuggest.meta.MetaTableKey
import tech.mlsql.autosuggest.utils.SchemaUtils

/**
 * 2019-07-18 WilliamZhu(allwefantasy@gmail.com)
 */
class SchemaRegistry(_spark: SparkSession) {
  val spark = _spark

  def createTableFromDBSQL(prefix: Option[String], db: Option[String], tableName: String, createSQL: String) = {
    val rd = new RDSchema(JdbcConstants.MYSQL)
    val tableName = rd.createTable(createSQL)
    val schema = rd.getTableSchema(tableName)
    val table = MetaTableKey(prefix, db, tableName)
    AutoSuggestContext.memoryMetaProvider.register(table, SchemaUtils.toMetaTable(table, schema));
  }

  def createTableFromHiveSQL(prefix: Option[String], db: Option[String], tableName: String, createSQL: String) = {
    spark.sql(createSQL)
    val schema = spark.table(tableName).schema
    val table = MetaTableKey(prefix, db, tableName)
    AutoSuggestContext.memoryMetaProvider.register(table, SchemaUtils.toMetaTable(table, schema));

  }


  def createTableFromJson(prefix: Option[String], db: Option[String], tableName: String, schemaJson: String) = {
    val schema = DataType.fromJson(schemaJson).asInstanceOf[StructType]
    val table = MetaTableKey(prefix, db, tableName)
    AutoSuggestContext.memoryMetaProvider.register(table, SchemaUtils.toMetaTable(table, schema));
  }


}
