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

package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.dsl.{ConnectMeta, DBMappingKey, ScriptSQLExec}

class MLSQLSolr(override val uid: String) extends MLSQLBaseStreamSource with WowParams {

  def this() = this(BaseParams.randomUID())

  override def fullFormat: String = "solr"

  override def shortFormat: String = "solr"

  override def dbSplitter: String = "/"

  def isStream: Boolean = {
    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.env().contains("streamName")
  }

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {

    // if contains splitter, then we will try to find dbname in dbMapping.
    parseRef(aliasFormat, config.path, dbSplitter, (options: Map[String, String]) => {
      reader.options(options)
    })

    val format = config.config.getOrElse("implClass", fullFormat)
    if (isStream) {
      // ignore the reader since this reader is not stream reader
      val streamReader = config.df.get.sparkSession.readStream
      streamReader.options(config.config).format(format).load()
    } else {
      reader.options(config.config).format(format).load()
    }
  }

  override def save(batchWriter: DataFrameWriter[Row], config: DataSinkConfig): Any = {

    // if contains splitter, then we will try to find dbname in dbMapping.
    parseRef(aliasFormat, config.path, dbSplitter, (options: Map[String, String]) => {
      batchWriter.options(options)
    })

    if (isStream) {
      super.save(batchWriter, config)
    }else{
      batchWriter.options(config.config).format(fullFormat).save()
    }
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {

    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(dbSplitter, 2)
    } else {
      Array("", config.path)
    }

    var table = _dbtable
    var dbName = _dbname

    val newOptions = scala.collection.mutable.HashMap[String, String]() ++ config.config
    ConnectMeta.options(DBMappingKey(shortFormat, _dbname)) match {
      case Some(option) =>
        dbName = ""
        newOptions ++= option

        table.split(dbSplitter) match {
          case Array(_db, _table) =>
            dbName = _db
            table = _table
          case _ =>
        }

      case None =>
      //dbName = ""
    }


    newOptions.filter(f => f._1 == "collection").map { f =>
      if (f._2.contains(dbSplitter)) {
        f._2.split(dbSplitter, 2) match {
          case Array(_db, _table) =>
            dbName = _db
            table = _table
          case Array(_db) =>
            dbName = _db
        }
      } else {
        dbName = f._2
      }
    }

    SourceInfo(shortFormat, dbName, table)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

}
