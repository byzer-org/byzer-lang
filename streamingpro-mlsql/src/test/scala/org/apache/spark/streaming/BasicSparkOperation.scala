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

package org.apache.spark.streaming

import java.io.File

import net.csdn.common.reflect.ReflectHelper
import org.apache.commons.io.FileUtils
import org.scalatest.{FlatSpec, FunSuite, Matchers}
import serviceframework.dispatcher.{Compositor, StrategyDispatcher}
import streaming.common.ParamsUtil
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}

/**
  * Created by allwefantasy on 30/3/2017.
  */
trait BasicSparkOperation extends FlatSpec with Matchers {

  def withBatchContext[R](runtime: SparkRuntime)(block: SparkRuntime => R): R = {
    try {
      block(runtime)
    } finally {
      try {
        StrategyDispatcher.clear
        PlatformManager.clear
        runtime.destroyRuntime(false, true)
        val db = new File("./metastore_db")
        if (db.exists()) {
          FileUtils.deleteDirectory(db)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  def getCompositorParam(item: Compositor[_]) = {
    ReflectHelper.field(item, "_configParams").
      asInstanceOf[java.util.List[java.util.Map[Any, Any]]]
  }

  def setupBatchContext(batchParams: Array[String], configFilePath: String = null) = {
    var params: ParamsUtil = null
    if (configFilePath != null) {
      val extraParam = Array("-streaming.job.file.path", configFilePath)
      params = new ParamsUtil(batchParams ++ extraParam)
    } else {
      params = new ParamsUtil(batchParams)
    }
    PlatformManager.getOrCreate.run(params, false)
    val runtime = PlatformManager.getRuntime.asInstanceOf[SparkRuntime]
    runtime
  }

  def appWithBatchContext(batchParams: Array[String], configFilePath: String) = {
    var runtime: SparkRuntime = null
    try {
      val extraParam = Array("-streaming.job.file.path", configFilePath)
      val params = new ParamsUtil(batchParams ++ extraParam)
      PlatformManager.getOrCreate.run(params, false)
      runtime = PlatformManager.getRuntime.asInstanceOf[SparkRuntime]
    } finally {
      try {
        StrategyDispatcher.clear
        PlatformManager.clear
        if (runtime != null) {
          runtime.destroyRuntime(false, true)
        }
        FileUtils.deleteDirectory(new File("./metastore_db"))
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }


}
