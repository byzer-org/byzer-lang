package tech.mlsql.autosuggest.meta

case class MetaTableKey(prefix: Option[String], db: Option[String], table: String)

case class MetaTable(key: MetaTableKey, columns: List[MetaTableColumn])

case class MetaTableColumn(name: String, dataType: String, isNull: Boolean, extra: Map[String, String])
