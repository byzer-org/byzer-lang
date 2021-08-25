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

import org.apache.http.client.fluent.Request
import org.apache.http.entity.ContentType
import streaming.log.WowLog
import tech.mlsql.arrow.python.runner.PythonConf
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.net.NetTool

trait BaseHttpLogClient extends Logging with WowLog {

  val _conf: Map[String, String] = BaseHttpLogClient.conf

  def write(iter: Iterator[String], params: Map[String, String]): Unit = {
    val owner = params.getOrElse(ScalaMethodMacros.str(PythonConf.PY_EXECUTE_USER), "")
    val groupId = params.getOrElse("groupId", "")
    try {
      val url = _conf.getOrElse("spark.mlsql.log.driver.url", NetTool.localHostNameForURI())
      val token = _conf.getOrElse("spark.mlsql.log.driver.token", "mlsql")
      iter.foreach { line =>
        val body = SendLog(token, LogUtils.formatWithOwner(line, owner, groupId)).json
        Request.Post(url).bodyString(body, ContentType.APPLICATION_JSON.withCharset("utf8"))
          .addHeader("Content-Type", "application/x-www-form-urlencoded")
          .execute()
      }

    } catch {
      case e: Exception =>
        logError(LogUtils.formatWithOwner(s"Failed to write log on client side!", owner, groupId), e)
    }
  }

}

object BaseHttpLogClient {
  var conf: Map[String, String] = _

  def init(conf: Map[String, String]): Any = {
    this.conf = conf
  }
}