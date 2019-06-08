/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  def is_2_3_X() = {
    version == Core_2_3_x.coreVersion
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
