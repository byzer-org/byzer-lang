package tech.mlsql.datasource

case class TableMetaInfo(db: String, table: String, schema: String)

object MLSQLMultiDeltaOptions {
  val META_KEY = "__meta__"
  val KEEP_BINLOG_IN_META = "keepBinlogInMeta"
  val FULL_PATH_KEY = "__path__"
}