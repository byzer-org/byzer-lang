package tech.mlsql.indexer.impl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import tech.mlsql.indexer.MLSQLIndexer

/**
 * 29/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class MySQLIndexer extends MLSQLIndexer{
  override def rewrite(sql: LogicalPlan, options: Map[String, String]): LogicalPlan = ???

  override def read(sql: LogicalPlan, options: Map[String, String]): Option[DataFrame] = ???

  override def write(df: DataFrame, options: Map[String, String]): Option[DataFrame] = ???
}
