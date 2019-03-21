package streaming.test.datasource.help

import streaming.dsl.auth.MLSQLTable

/**
  * 2019-03-21 WilliamZhu(allwefantasy@gmail.com)
  */
object MLSQLTableEnhancer {


  implicit def mslqlTableEnhance(table: MLSQLTable) = {
    new MLSQLTableEnhancer(table)
  }

}

class MLSQLTableEnhancer(table: MLSQLTable) {
  def isSourceTypeOf(name: String) = {
    table.sourceType.map(m => m == name).isDefined
  }
}
