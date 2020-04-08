package org.apache.spark

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.ExplainCommand

/**
 * 2019-08-16 WilliamZhu(allwefantasy@gmail.com)
 */
object MLSQLSparkUtils {
  def rpcEnv() = {
    SparkEnv.get.rpcEnv
  }

  def createExplainCommand(lg: LogicalPlan, extended: Boolean) = {
    ExplainCommand(lg, extended = extended)
  }
}
