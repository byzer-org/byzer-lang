package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * 2019-04-17 WilliamZhu(allwefantasy@gmail.com)
  */
object SparkExposure {
  def cleanCache(spark: SparkSession, plan: LogicalPlan) = {
    spark.sqlContext.sharedState.cacheManager.uncacheQuery(spark, plan, true)
  }
}
