package tech.mlsql.autosuggest

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalog.{Catalog, Column, Table}

/**
 * 9/3/2023 WilliamZhu(allwefantasy@gmail.com)
 */
class CatalogWrap(catalog: Catalog) {
  def listTables(): List[Table] = {
    catalog.listTables().collect().toList
  }

  def listColumns(table: String): Dataset[Column] = {
    catalog.listColumns(table)
  }

  def getTable(table: String): Table = {
    catalog.getTable(table)
  }

  def tableExists(table: String): Boolean = {
    try {
      catalog.tableExists(table)
    } catch {
      case e: Exception => false
    }
  }

}
