package tech.mlsql.indexer.impl

import tech.mlsql.indexer.{MLSQLIndexerMeta, MlsqlIndexer, MlsqlOriTable}

/**
 * 21/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class TestIndexerMeta extends MLSQLIndexerMeta {
  override def fetchIndexers(tableNames: List[MlsqlOriTable], options: Map[String, String]): Map[MlsqlOriTable, MlsqlIndexer] = {
    //     Map(
    //       MlsqlOriTable(
    //
    //       )-> MlsqlIndexer()
    //     )
    Map()
  }
}
