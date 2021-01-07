package tech.mlsql.indexer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * 18/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait MLSQLIndexer {
  def rewrite(sql: LogicalPlan, options: Map[String, String]): LogicalPlan

  def read(sql:LogicalPlan,options: Map[String, String]): Option[DataFrame]

  def write(df: DataFrame, options: Map[String, String]): Option[DataFrame]
}
