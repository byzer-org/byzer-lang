package tech.mlsql.autosuggest.utils

import org.apache.spark.sql.types.StructType
import tech.mlsql.autosuggest.meta.{MetaTable, MetaTableColumn, MetaTableKey}

/**
 * 15/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object SchemaUtils {
  def toMetaTable(table: MetaTableKey, st: StructType) = {
    val columns = st.fields.map { item =>
      MetaTableColumn(item.name, item.dataType.typeName, item.nullable, Map())
    }.toList
    MetaTable(table, columns)
  }

}
