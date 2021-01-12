package tech.mlsql.indexer

/**
 * 18/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait MLSQLIndexerMeta {
  def fetchIndexers(tableNames: List[MlsqlOriTable], options: Map[String, String]): Map[MlsqlOriTable, List[MlsqlIndexer]]

  def registerIndexer(indexer: MlsqlIndexer): Unit
}
