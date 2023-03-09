package tech.mlsql.autosuggest.meta

import tech.mlsql.autosuggest.{AutoSuggestContext, CatalogWrap, SpecialTableConst}


/**
 * 8/11/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class RuntimeMetaProvider extends MetaProvider {
  
  override def search(key: MetaTableKey, extra: Map[String, String] = Map()): Option[MetaTable] = {
    val context = AutoSuggestContext.context()
    val catalog = new CatalogWrap(context.session.catalog)
    if (!catalog.tableExists(key.table)) {
      return None
    }
    val table = catalog.getTable(key.table)
    val columns = catalog.listColumns(table.name).collect().toList
    val metaTableColumns = columns.map { col =>
      MetaTableColumn(col.name, col.dataType, col.nullable, Map())
    }

    val dbType = if(table.isTemporary) {
      Some(SpecialTableConst.TEMP_TABLE_DB_KEY)
    }else None

    Some(MetaTable(MetaTableKey(None, dbType, table.name), metaTableColumns))
  }

  def register(name: String, metaTable: MetaTable) = {
    this
  }

  override def list(extra: Map[String, String] = Map()): List[MetaTable] = {
    val context = AutoSuggestContext.context()
    val catalog = context.session.catalog
    val tables = catalog.listTables().collect().toList
    tables.filter(item=> !item.name.matches("[a-z0-9]{32}") ).map { _table =>
      val table = catalog.getTable(_table.name)
      val columns = catalog.listColumns(table.name).collect().toList
      val metaTableColumns = columns.map { col =>
        MetaTableColumn(col.name, col.dataType, col.nullable, Map())
      }

      val dbType = if(table.isTemporary) {
        Some(SpecialTableConst.TEMP_TABLE_DB_KEY)
      }else None

      MetaTable(MetaTableKey(None, dbType, table.name), metaTableColumns)
    }
  }
}