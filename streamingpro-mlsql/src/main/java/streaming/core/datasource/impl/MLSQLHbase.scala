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

import _root_.streaming.core.datasource._
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import _root_.streaming.dsl.{ConnectMeta, DBMappingKey}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql._

/**
  * Created by latincross on 12/29/2018.
  */
class MLSQLHbase(override val uid: String) extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry with WowParams {
  def this() = this(BaseParams.randomUID())


  override def fullFormat: String = "org.apache.spark.sql.execution.datasources.hbase"

  override def shortFormat: String = "hbase"

  override def dbSplitter: String = ":"

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(dbSplitter, 2)
    } else {
      Array("", config.path)
    }

    var namespace = ""

    val format = config.config.getOrElse("implClass", fullFormat)
    if (_dbname != "") {
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
        if (options.contains("namespace")) {
          namespace = options("namespace")
        }
        reader.options(options)
      })
    }

    if (config.config.contains("namespace")) {
      namespace = config.config("namespace")
    }

    val inputTableName = if (namespace == "") _dbtable else s"${namespace}:${_dbtable}"

    reader.option("inputTableName", inputTableName)

    //load configs should overwrite connect configs
    reader.options(config.config)
    reader.format(format).load()
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(dbSplitter, 2)
    } else {
      Array("", config.path)
    }

    var namespace = ""

    val format = config.config.getOrElse("implClass", fullFormat)
    if (_dbname != "") {
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
        if (options.contains("namespace")) {
          namespace = options.get("namespace").get
        }
        writer.options(options)
      })
    }

    if (config.config.contains("namespace")) {
      namespace = config.config.get("namespace").get
    }

    val outputTableName = if (namespace == "") _dbtable else s"${namespace}:${_dbtable}"

    writer.mode(config.mode)
    writer.option("outputTableName", outputTableName)
    //load configs should overwrite connect configs
    writer.options(config.config)
    config.config.get("partitionByCol").map { item =>
      writer.partitionBy(item.split(","): _*)
    }
    writer.format(config.config.getOrElse("implClass", fullFormat)).save()
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val format = config.config.getOrElse("implClass", fullFormat)
    val Array(connect, namespace, table) = if (config.path.contains(dbSplitter)) {
      config.path.split(dbSplitter) match {
        case Array(connect, namespace, table) => Array(connect, namespace, table)
        case Array(connectOrNameSpace, table) =>
          ConnectMeta.presentThenCall(DBMappingKey(format, connectOrNameSpace), (op) => {}) match {
            case Some(i) => Array(connectOrNameSpace, "", table)
            case None => Array("", connectOrNameSpace, table)
          }
        case Array(connect, namespace, table, _*) => Array(connect, namespace, table)
      }
    } else {
      Array("", "", config.path)
    }


    var finalNameSpace = config.config.getOrElse("namespace", namespace)

    ConnectMeta.presentThenCall(DBMappingKey(format, connect), (options) => {
      if (options.contains("namespace")) {
        finalNameSpace = options.get("namespace").get
      }

    })


    SourceInfo(shortFormat, finalNameSpace, table)
  }

  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  final val zk: Param[String] = new Param[String](this, "zk", "zk address")
  final val family: Param[String] = new Param[String](this, "family", "default cf")
}
