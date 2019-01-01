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
import streaming.dsl.{ConnectMeta, DBMappingKey}

class MLSQLSolr extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry {

  override def fullFormat: String = "solr"

  override def shortFormat: String = "solr"

  override def dbSplitter: String = "/"

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {

    var dbtable = config.path
    // if contains splitter, then we will try to find dbname in dbMapping.
    val format = config.config.getOrElse("implClass", fullFormat)
    if (config.path.contains(dbSplitter)) {
      val Array(_dbname, _dbtable) = config.path.split(dbSplitter, 2)

      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
        dbtable = _dbtable
        reader.options(options)
      })
    }

    //load configs should overwrite connect configs
    reader.options(config.config)
    reader.format(format).load(dbtable)
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    var dbtable = config.path
    // if contains splitter, then we will try to find dbname in dbMapping.

    val format = config.config.getOrElse("implClass", fullFormat)
    if (config.path.contains(dbSplitter)) {
      val Array(_dbname, _dbtable) = config.path.split(dbSplitter, 2)
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), (options) => {
        dbtable = _dbtable
        writer.options(options)
      })
    }
    writer.mode(config.mode)
    //load configs should overwrite connect configs
    writer.options(config.config)
    config.config.get("partitionByCol").map { item =>
      writer.partitionBy(item.split(","): _*)
    }
    writer.format(config.config.getOrElse("implClass", fullFormat)).save(dbtable)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {

    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(dbSplitter, 2)
    } else {
      Array("", config.path)
    }

    val db = if (config.config.contains("collection")) {
      config.config.get("collection").get
    } else {
      val format = config.config.getOrElse("implClass", fullFormat)

      ConnectMeta.options(DBMappingKey(format, _dbname)).get("collection")
    }

    SourceInfo(shortFormat, db, "")
  }

}
