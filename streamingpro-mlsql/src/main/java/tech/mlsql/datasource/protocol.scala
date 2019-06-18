package tech.mlsql.datasource

case class TableMetaInfo(db: String, table: String, schema: String)

object MLSQLMultiDeltaOptions {
  val META_KEY = "__meta__"
}