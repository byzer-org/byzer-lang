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

package streaming.dsl.mmlib.algs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.log.Logging

/**
  * Created by allwefantasy on 25/8/2018.
  */
class SQLJDBC extends SQLAlg with MllibFunctions with Functions with Logging {

  def executeInDriver(options: Map[String, String]) = {
    val driver = options("driver")
    val url = options("url")
    Class.forName(driver)
    val connection = java.sql.DriverManager.getConnection(url, options("user"), options("password"))
    try {
      // we suppose that there is only one create if
      val statements = options.filter(f =>"""driver\-statement\-[0-9]+""".r.findFirstMatchIn(f._1).nonEmpty).
        map(f => (f._1.split("-").last.toInt, f._2)).toSeq.sortBy(f => f._1).map(f => f._2).map { f =>
        logInfo(s"${getClass.getName} execute: ${f}")
        connection.prepareStatement(f)
      }

      statements.map { f =>
        f.execute()
        f
      }.map(_.close())
    } finally {
      if (connection != null)
        connection.close()
    }

  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    var _params = params
    if (ScriptSQLExec.dbMapping.containsKey(path)) {
      ScriptSQLExec.dbMapping.get(path).foreach { f =>
        _params = _params + (f._1 -> f._2)
      }
    }
    executeInDriver(_params)
    emptyDataFrame()(df)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"${getClass.getName} not support register ")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support predict function.")
  }
}
