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

package streaming.core

import java.util.{Map => JMap}

import org.apache.http.client.fluent.Request
import serviceframework.dispatcher.StrategyDispatcher
import streaming.common.DefaultShortNameMapping
import streaming.core.strategy.platform.{PlatformManager, StreamingRuntime}
import tech.mlsql.common.utils.hdfs.HDFSOperator

import scala.collection.JavaConversions._

/**
  * 5/2/16 WilliamZhu(allwefantasy@gmail.com)
  */
object Dispatcher {
  def dispatcher(contextParams: JMap[Any, Any]): StrategyDispatcher[Any] = {
    val defaultShortNameMapping = new DefaultShortNameMapping()
    if (contextParams != null && contextParams.containsKey("streaming.job.file.path")) {
      val runtime = contextParams.get("_runtime_").asInstanceOf[StreamingRuntime]


      val jobFilePath = contextParams.get("streaming.job.file.path").toString

      var jobConfigStr = "{}"

      if (jobFilePath.toLowerCase().startsWith("classpath://")) {
        val cleanJobFilePath = jobFilePath.substring("classpath://".length)
        jobConfigStr = scala.io.Source.fromInputStream(
          Dispatcher.getClass.getResourceAsStream(cleanJobFilePath)).getLines().
          mkString("\n")
      } else if (jobFilePath.toLowerCase().startsWith("http://") || jobFilePath.toLowerCase().startsWith("https://")) {
        jobConfigStr = Request.Get(jobFilePath)
          .connectTimeout(30000)
          .socketTimeout(30000)
          .execute().returnContent().asString();
      }
      else {
        jobConfigStr = HDFSOperator.readFile(jobFilePath)
      }

      if (jobConfigStr == null || jobConfigStr.isEmpty)
        jobConfigStr = "{}"

      StrategyDispatcher.getOrCreate(jobConfigStr, defaultShortNameMapping)
    } else {
      StrategyDispatcher.getOrCreate("{}", defaultShortNameMapping)
    }

  }

  def contextParams(jobName: String) = {
    val runtime = PlatformManager.getRuntime
    val tempParams: java.util.Map[Any, Any] = runtime.params
    val contextParams: java.util.HashMap[Any, Any] = new java.util.HashMap[Any, Any]()
    tempParams.foreach(f => contextParams += (f._1 -> f._2))
    contextParams.put("_client_", jobName)
    contextParams.put("_runtime_", runtime)
    contextParams
  }
}
