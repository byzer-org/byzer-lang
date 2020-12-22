package tech.mlsql.indexer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * 22/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait IndexerSelector {
  def rewrite(lp: LogicalPlan, options: Map[String, String]): LogicalPlan
}
