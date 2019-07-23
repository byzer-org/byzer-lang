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

import org.apache.spark.ml.param.Param
import org.apache.spark.sql._
import _root_.streaming.core.datasource._
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import _root_.streaming.dsl.{ConnectMeta, DBMappingKey}

class MLSQLMongo (override val uid: String) extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry with WowParams {
  def this() = this(BaseParams.randomUID())


  override def fullFormat: String = "com.mongodb.spark.sql"

  override def shortFormat: String = "mongo"

  override def dbSplitter: String = "/"

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    var dbtable = config.path
    // if contains splitter, then we will try to find dbname in dbMapping.
    // otherwize we will do nothing since mongo use something like collection
    // it will do no harm.
    val format = config.config.getOrElse("implClass", fullFormat)
    if (config.path.contains(dbSplitter)) {
      val Array(_dbname, _dbtable) = config.path.split(dbSplitter, 2)
      dbtable = _dbtable
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
        reader.options(options)
      })
    }

    reader.option("collection", dbtable)
    //load configs should overwrite connect configs
    reader.options(config.config)
    reader.format(format).load()
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    var dbtable = config.path
    // if contains splitter, then we will try to find dbname in dbMapping.
    // otherwize we will do nothing since mongo use something like collection
    // it will do no harm.
    val format = config.config.getOrElse("implClass", fullFormat)
    if (config.path.contains(dbSplitter)) {
      val Array(_dbname, _dbtable) = config.path.split(dbSplitter, 2)
      dbtable = _dbtable
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
        writer.options(options)
      })
    }
    writer.mode(config.mode)
    writer.option("collection", dbtable)
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
    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(dbSplitter, 2)
    }else{
      Array("" ,config.path)
    }

    var db = ""

    if(config.config.contains("spark.mongodb.input.database")){
      db = config.config.get("spark.mongodb.input.database").get
    }else if (config.config.contains("spark.mongodb.output.database")){
      db = config.config.get("spark.mongodb.output.database").get
    }else if(config.config.contains("database")){
      db = config.config.get("database").get
    }else{
      val uri = if (config.config.contains("uri")){
        config.config.get("uri").get
      }else{
        val format = config.config.getOrElse("implClass", fullFormat)

        ConnectMeta.options(DBMappingKey(format, _dbname)).get("uri")
      }

      db = uri.substring(uri.lastIndexOf('/') + 1).takeWhile(_ != '?')
    }

    SourceInfo(shortFormat ,db ,_dbtable)
  }

  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  final val partitioner: Param[String] = new Param[String](this, "partitioner", "Optional. e.g. MongoPaginateBySizePartitioner")
  final val uri: Param[String] = new Param[String](this, "uri", "Required. e.g. mongodb://127.0.0.1:27017/twitter")
}
