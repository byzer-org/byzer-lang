package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}

/**
  * Created by allwefantasy on 7/8/2018.
  */
class WowFastLocalTableScanStrategies extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case LocalRelation(output, data, isStreaming) =>
      WowFastLocalTableScanExec(output, data, isStreaming) :: Nil
    case _ => Nil
  }
}
