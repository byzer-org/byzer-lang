package org.apache.spark

/**
  * Created by allwefantasy on 20/9/2018.
  */
object SparkCoreVersion {
  def version = {
    val coreVersion = org.apache.spark.SPARK_VERSION.split("\\.").take(2).mkString(".") + ".x"
    coreVersion
  }
}
