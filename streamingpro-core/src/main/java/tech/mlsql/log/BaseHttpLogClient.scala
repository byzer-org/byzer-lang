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

package tech.mlsql.log

import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.entity.ContentType
import org.apache.spark.SparkEnv
import streaming.log.WowLog
import tech.mlsql.arrow.python.runner.PythonConf
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.net.NetTool

trait BaseHttpLogClient extends Logging with WowLog {

  var _conf: Map[String, String] = _

  def write(iter: Iterator[String], params: Map[String, String]): Unit = {
    if (_conf == null) {
      synchronized {
        if (_conf == null) {
          _conf = SparkEnv.get.conf.getAll.toMap
        }
      }
    }
    val owner = params.getOrElse(ScalaMethodMacros.str(PythonConf.PY_EXECUTE_USER), "")
    val groupId = params.getOrElse("groupId", "")
    try {
      val url = _conf.getOrElse("spark.mlsql.log.driver.url", NetTool.localHostNameForURI())
      val token = _conf.getOrElse("spark.mlsql.log.driver.token", "mlsql")
      val enablePrint = _conf.getOrElse("spark.mlsql.log.driver.enablePrint", "false").toBoolean
      iter.foreach { line =>
        val logItem = LogUtils.formatWithOwner(line, owner, groupId)
        if (enablePrint) {
          println(logItem)
        } else {
          val body = SendLog(token, logItem).json
          Request.Post(url).addHeader("Content-Type", "application/json").
            socketTimeout(500).
            connectTimeout(100)
            .bodyString(body, ContentType.APPLICATION_JSON.withCharset("utf8"))
            .execute()
        }
      }

    } catch {
      case e: Exception =>
        logError(LogUtils.formatWithOwner(s"Failed to write log on client side!", owner, groupId), e)
    }
  }

}