package tech.mlsql.runtime
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

import org.apache.commons.lang3.StringUtils
import streaming.core.strategy.platform.{SparkRuntime, StreamingRuntime}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.job.{JobManager, RunScriptExecutor}

import scala.io.Source

/**
 * When using this plugin, please set these parameters as shown below:
 * streaming.rest=false
 * streaming.spark.service=false
 * streaming.mlsql.script.path=the file path of mlsql script (support for hdfs://, file://, http://)
 * streaming.runtime_hooks=tech.mlsql.runtime.SparkSubmitMLSQLScriptRuntimeLifecycle
 * streaming.mlsql.script.owner=XXXXX
 * streaming.mlsql.script.jobName=SparkSubmitTest
 */
class SparkSubmitMLSQLScriptRuntimeLifecycle extends MLSQLPlatformLifecycle with Logging {

  override def beforeRuntime(params: Map[String, String]): Unit = {}

  override def afterRuntime(runtime: StreamingRuntime, params: Map[String, String]): Unit = {  }


  override def beforeDispatcher(runtime: StreamingRuntime, params: Map[String, String]): Unit = {}

  override def afterDispatcher(runtime: StreamingRuntime, params: Map[String, String]): Unit = {
    val mlsql_path = params.getOrElse("streaming.mlsql.script.path", "")
    if (StringUtils.isEmpty(mlsql_path)) {
      logWarning(s"The value of parameter 'streaming.mlsql.script.path' is empty.")
      return
    }
    val rootSparkSession = runtime.asInstanceOf[SparkRuntime].sparkSession
    JobManager.init(rootSparkSession)
    try {
      val sql = new StringBuffer()
      if (!mlsql_path.startsWith("http")) {
        val array = rootSparkSession.sparkContext.textFile(mlsql_path).collect()
        array.foreach(
          line => {
            sql.append(line).append("\n")
          }
        )
      } else {
        val file = Source.fromURL(mlsql_path)
        for (line <- file.getLines()) {
          sql.append(line).append("\n")
        }
      }

      if (StringUtils.isEmpty(sql.toString)) {
        logWarning(s"The mlsql script file is empty.")
        return
      }

      val executor = new RunScriptExecutor(
        params ++ Map("sql" -> sql.toString,
          "owner" -> params.getOrElse("streaming.mlsql.script.owner", "admin"),
          "async" -> "false",
          "executeMode" -> params.getOrElse("streaming.mlsql.script.executeMode", "query"),
          "jobName" -> params.getOrElse("streaming.mlsql.script.jobName", "SparkSubmitMLSQLScriptRuntimeJob")))
      executor.execute()
    } catch {
      case e: Exception =>
        logError(s"Run script ${mlsql_path} error: ", e)
    } finally {
      JobManager.shutdown
    }
  }
}
