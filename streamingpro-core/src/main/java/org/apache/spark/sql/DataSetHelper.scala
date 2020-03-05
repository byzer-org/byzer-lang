package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * 2019-06-19 WilliamZhu(allwefantasy@gmail.com)
  */
object DataSetHelper {
  def create(sparkSession: SparkSession, logicalPlan: LogicalPlan) = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }
}
