package org.apache.spark

/**
  * Created by allwefantasy on 20/9/2018.
  */
object SparkCoreVersion {
  def version = {
    val coreVersion = org.apache.spark.SPARK_VERSION.split("\\.").take(2).mkString(".") + ".x"
    coreVersion
  }

  def exactVersion = {
    val coreVersion = org.apache.spark.SPARK_VERSION
    coreVersion
  }

  def is_2_2_X() = {
    version == VERSION_2_2_X
  }

  def is_2_3_X() = {
    version == VERSION_2_3_X
  }

  def is_2_4_X() = {
    version == VERSION_2_4_X
  }

  val VERSION_2_2_X = "2.2.x"
  val VERSION_2_3_X = "2.3.x"
  val VERSION_2_4_X = "2.4.x"
}
