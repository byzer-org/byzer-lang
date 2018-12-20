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
import streaming.dsl.ScriptSQLExec

class MLSQLElasticSearch extends MLSQLSource with MLSQLSink with MLSQLRegistry {


  override def fullFormat: String = "org.elasticsearch.spark.sql"

  override def shortFormat: String = "es"

  override def dbSplitter: String = "/"

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    var dbtable = config.path
    // if contains splitter, then we will try to find dbname in dbMapping.
    // otherwize we will do nothing since elasticsearch use something like index/type
    // it will do no harm.
    if (config.path.contains(dbSplitter)) {
      val Array(_dbname, _dbtable) = config.path.split(dbSplitter, 2)
      if (ScriptSQLExec.dbMapping.containsKey(_dbname)) {
        dbtable = _dbtable
        ScriptSQLExec.dbMapping.get(_dbname).foreach { f =>
          reader.option(f._1, f._2)
        }
      }
    }
    //load configs should overwrite connect configs
    reader.options(config.config)
    reader.format(config.config.getOrElse("implClass", fullFormat)).load(dbtable)
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    var dbtable = config.path
    // if contains splitter, then we will try to find dbname in dbMapping.
    // otherwize we will do nothing since elasticsearch use something like index/type
    // it will do no harm.
    if (config.path.contains(dbSplitter)) {
      val Array(_dbname, _dbtable) = config.path.split(dbSplitter, 2)
      if (ScriptSQLExec.dbMapping.containsKey(_dbname)) {
        dbtable = _dbtable
        ScriptSQLExec.dbMapping.get(_dbname).foreach { f =>
          writer.option(f._1, f._2)
        }
      }
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
    DataSourceRegistry.register(fullFormat, this)
    DataSourceRegistry.register(shortFormat, this)
  }
}
