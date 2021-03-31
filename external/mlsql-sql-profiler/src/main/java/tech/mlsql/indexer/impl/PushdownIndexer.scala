package tech.mlsql.indexer.impl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import tech.mlsql.indexer.MLSQLIndexer
import org.apache.spark.sql.optimizer.Pushdown

class PushdownIndexer  extends MLSQLIndexer {
  override def rewrite(lp: LogicalPlan, options: Map[String, String]): LogicalPlan = {
    Pushdown.apply(lp)
  }

  override def read(sql: LogicalPlan, options: Map[String, String]): Option[DataFrame] = ???

  override def write(df: DataFrame, options: Map[String, String]): Option[DataFrame] = ???
}

