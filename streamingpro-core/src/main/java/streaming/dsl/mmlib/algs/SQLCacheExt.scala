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

import org.apache.spark.ml.param.{BooleanParam, Param}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkExposure, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.param.WowParams
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging

import scala.collection.mutable.ArrayBuffer


class SQLCacheExt(override val uid: String) extends SQLAlg with WowParams {

  override def train(_df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val df = if (path.isEmpty) _df else _df.sparkSession.table(path)
    val exe = params.get(execute.name).getOrElse {
      "cache"
    }

    val _lifeTime = CacheLifeTime.withName(params.get(lifeTime.name).getOrElse {
      CacheLifeTime.SCRIPT.toString
    })


    val __dfname__ = params("__dfname__")
    val _isEager = params.get(isEager.name).map(f => f.toBoolean).getOrElse(false)

    if (!execute.isValid(exe)) {
      throw new MLSQLException(s"${execute.name} should be cache or uncache")
    }

    val context = ScriptSQLExec.contextGetOrForTest()

    if (exe == "cache") {
      df.persist()
      SQLCacheExt.addCache(TableCacheItem(context.groupId, context.owner, __dfname__,
        df.queryExecution.analyzed,
        _lifeTime,
        System.currentTimeMillis()))
    } else {
      df.unpersist()
    }

    if (_isEager) {
      df.count()
    }
    df
  }


  override def skipPathPrefix: Boolean = true

  override def skipOriginalDFName: Boolean = false

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException("register is not support")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }

  final val execute: Param[String] = new Param[String](this, "execute", "cache|uncache", isValid = (m: String) => {
    m == "cache" || m == "uncache"
  })

  final val isEager: BooleanParam = new BooleanParam(this, "isEager", "if set true, execute computing right now, and cache the table")
  final val lifeTime: Param[String] = new Param[String](this, "lifeTime", "script|session|application", isValid = (m: String) => {
    m == "script" || m == "session" || m == "application"
  })


  override def doc: Doc = Doc(MarkDownDoc,
    """
      |SQLCacheExt is used to cache/uncache table.
      |
      |```sql
      |run table as CacheExt.`` where execute="cache" and isEager="true";
      |```
      |
      |If you execute the upper command, then table will be cached immediately, othersise only the second time
      |to use the table you will fetch the table from cache.
      |
      |To release the table , do like this:
      |
      |```sql
      |run table as CacheExt.`` where execute="uncache";
      |```
    """.stripMargin)

  override def modelType: ModelType = ProcessType

  def this() = this(WowParams.randomUID())

  override def explainParams(sparkSession: SparkSession): DataFrame = _explainParams(sparkSession)

}

object SQLCacheExt extends Logging with WowLog {
  val cache = new java.util.concurrent.ConcurrentHashMap[String, ArrayBuffer[TableCacheItem]]()


  def addCache(tci: TableCacheItem) = {
    synchronized {
      val items = cache.getOrDefault(tci.groupId, ArrayBuffer[TableCacheItem]())
      items += tci
      cache.put(tci.groupId, items)
    }
  }

  def cleanCache(session: SparkSession, groupId: String) = {
    val items = cache.remove(groupId)
    if (items != null) {
      items.foreach { item =>
        if (item.lifeTime == CacheLifeTime.SCRIPT) {
          logInfo(format(s"clean cache in ${groupId}: ${item.tableName}"))
          SparkExposure.cleanCache(session, item.planToCache)
        }
      }
    }
  }
}

object CacheLifeTime extends Enumeration {
  type lifetime = Value
  val SCRIPT = Value("script")
  val APPLICATION = Value("application")
  val SESSION = Value("session")
}

case class TableCacheItem(groupId: String,
                          owner: String,
                          tableName: String,
                          planToCache: LogicalPlan,
                          lifeTime: CacheLifeTime.Value,
                          cacheStartTime: Long)
