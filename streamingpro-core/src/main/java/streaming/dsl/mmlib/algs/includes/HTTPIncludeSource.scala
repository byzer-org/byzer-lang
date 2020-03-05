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

package streaming.dsl.mmlib.algs.includes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.mlsql.session.MLSQLException
import streaming.dsl.{IncludeSource, ScriptSQLExec}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.crawler.HttpClientCrawler

/**
  * Created by allwefantasy on 30/8/2018.
  */
class HTTPIncludeSource extends IncludeSource with Logging {
  override def fetchSource(sparkSession: SparkSession, path: String, options: Map[String, String]): String = {

    val context = ScriptSQLExec.context()

    var params = scala.collection.mutable.HashMap(
      "path" -> path
    )
    if (context.owner != null) {
      params += ("owner" -> context.owner)
    }

    options.filter(f => f._1.startsWith("param.")).map(f => (f._1.substring("param.".length), f._2)).foreach { f =>
      params += f
    }

    val method = options.getOrElse("method", "get")
    val fetch_url = context.userDefinedParam.getOrElse("__default__include_fetch_url__", path)
    val projectName = context.userDefinedParam.getOrElse("__default__include_project_name__", null)

    if (projectName != null) {
      params += ("projectName" -> projectName)
    }

    logInfo(s"""HTTPIncludeSource URL: ${fetch_url}  PARAMS:${params.map(f => s"${f._1}=>${f._2}").mkString(";")}""")
    val res = HttpClientCrawler.requestByMethod(fetch_url, method, params.toMap)
    if (res == null) {
      throw new MLSQLException(
        s"""
           |MLSQL engine fails to fetch script from ${fetch_url}.
           |PARAMS:\n${params.map(f => s"${f._1}=>${f._2}").mkString("\n")}
         """.stripMargin)
    }
    res

  }

  override def skipPathPrefix: Boolean = true
}
