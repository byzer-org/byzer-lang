package org.apache.spark.execution

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestHelper {
  def isCacheBuild(df: DataFrame)(implicit spark: SparkSession) = {
    spark.sharedState.cacheManager.lookupCachedData(df).get.cachedRepresentation.cacheBuilder.sizeInBytesStats.value > 0l
  }
}
