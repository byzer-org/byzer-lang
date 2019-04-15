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

package streaming.core.compositor.spark.transformation

import java.util

import org.apache.log4j.Logger
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper
import streaming.dsl.{ScriptSQLExec, ScriptSQLExecListener}
import tech.mlsql.job.{JobManager, MLSQLJobType}

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 5/7/2018.
  */
class MLSQLCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def sql = {
    _configParams.get(0).get("sql") match {
      case a: util.List[_] => Some(a.mkString(" "))
      case a: String => Some(a)
      case _ => None
    }
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    JobManager.init(sparkSession(params))
    require(sql.isDefined, "please set sql  by variable `sql` in config file")

    val _sql = translateSQL(sql.get, params)

    val jobInfo = JobManager.getJobInfo(
      "admin", MLSQLJobType.SCRIPT, "", _sql,
      -1L
    )
    JobManager.run(sparkSession(params), jobInfo, () => {
      val context = new ScriptSQLExecListener(sparkSession(params), "", Map())
      ScriptSQLExec.parse(_sql, context)
    })
    JobManager.shutdown
    if (middleResult == null) List() else middleResult
  }
}
