package org.apache.spark

import _root_.streaming.dsl.mmlib._

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
    version == Core_2_2_x.coreVersion
  }

  def is_2_3_1() = {
    exactVersion == Core_2_3_1.coreVersion
  }

  def is_2_3_2() = {
    exactVersion == Core_2_3_2.coreVersion
  }

  def is_2_4_X() = {
    version == Core_2_4_x.coreVersion
  }


}

object CarbonCoreVersion {
  def coreCompatibility(version: String, exactVersion: String) = {
    val vs = Seq(Core_2_2_x, Core_2_3_1).map(f => f.coreVersion).toSet
    vs.contains(version) || vs.contains(exactVersion)
  }
}
