package org.apache.spark

import org.apache.spark.util.WowCacheableClosureCleaner

/**
  * Created by allwefantasy on 6/8/2018.
  */
class WowFastSparkContext(sparkConf: SparkConf) extends SparkContext(sparkConf) {
  override private[spark] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    WowCacheableClosureCleaner.clean(f, checkSerializable)
    f
  }
}
