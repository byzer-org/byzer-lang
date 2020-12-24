package tech.mlsql.indexer.impl

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import tech.mlsql.indexer.{IndexerSelector, MLSQLIndexer, MLSQLIndexerMeta}

/**
 * 22/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class LinearTryIndexerSelector(indexers: Seq[MLSQLIndexer], indexerMeta: MLSQLIndexerMeta) extends IndexerSelector {
  override def rewrite(lp: LogicalPlan, options: Map[String, String]): LogicalPlan = {
    var newLP = lp
    indexers.foreach { indexer =>
      newLP = indexer.rewrite(lp, Map())
      if (lp != newLP) {
        return newLP
      }
    }
    return lp
  }
}
