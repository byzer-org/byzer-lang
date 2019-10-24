package tech.mlsql.datasource

case class TableMetaInfo(db: String, table: String, schema: String)

object MLSQLMultiDeltaOptions {
  val META_KEY = "__meta__"
  val KEEP_BINLOG = "keepBinlog"
  val FULL_PATH_KEY = "__path__"
  val BINLOG_PATH = "binlogPath"
}