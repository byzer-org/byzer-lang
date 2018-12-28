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

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.{ConnectMeta, DBMappingKey}

class MLSQLJDBC extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry {


  override def fullFormat: String = "jdbc"

  override def shortFormat: String = fullFormat

  override def dbSplitter: String = "."

  def toSplit = "\\."

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    var dbtable = config.path
    // if contains splitter, then we will try to find dbname in dbMapping.
    // otherwize we will do nothing since elasticsearch use something like index/type
    // it will do no harm.
    val format = config.config.getOrElse("implClass", fullFormat)
    if (config.path.contains(dbSplitter)) {
      val Array(_dbname, _dbtable) = config.path.split(toSplit, 2)
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
        dbtable = _dbtable
        reader.options(options)
      })
    }
    //load configs should overwrite connect configs
    reader.options(config.config)
    reader.option("dbtable", dbtable)
    var table = reader.format(format).load(dbtable)
    val columns = table.columns
    val colNames = new Array[String](columns.length)
    for( i <- 0 to columns.length - 1){
      val (dbtable, column) = parseTableAndColumnFromStr(columns(i))
      colNames(i)=column
    }
    table.toDF(colNames: _*)
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    var dbtable = config.path
    // if contains splitter, then we will try to find dbname in dbMapping.
    // otherwize we will do nothing since elasticsearch use something like index/type
    // it will do no harm.
    val format = config.config.getOrElse("implClass", fullFormat)
    if (config.path.contains(dbSplitter)) {
      val Array(_dbname, _dbtable) = config.path.split(toSplit, 2)
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
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

    config.config.get("idCol").map { item =>
      import org.apache.spark.sql.jdbc.DataFrameWriterExtensions._
      val extraOptionsField = writer.getClass.getDeclaredField("extraOptions")
      extraOptionsField.setAccessible(true)
      val extraOptions = extraOptionsField.get(writer).asInstanceOf[scala.collection.mutable.HashMap[String, String]]
      val jdbcOptions = new JDBCOptions(extraOptions.toMap + ("dbtable" -> dbtable))
      writer.upsert(Option(item), jdbcOptions, config.df.get)
    }.getOrElse {
      writer.option("dbtable", dbtable)
      writer.format(format).save(dbtable)
    }
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }
  def parseTableAndColumnFromStr(str: String): (String, String) = {
    val cleanedStr = cleanStr(str)
    val dbAndTable = cleanedStr.split("\\.")
    if (dbAndTable.length > 1) {
      val table = dbAndTable(0)
      val column = dbAndTable.splitAt(1)._2.mkString(".")
      (table, column)
    } else {
      (cleanedStr, cleanedStr)
    }
  }
  def cleanStr(str: String): String = {
    if (str.startsWith("`") || str.startsWith("\""))
      str.substring(1, str.length - 1)
    else str
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val Array(_dbname, _dbtable) =  if (config.path.contains(dbSplitter)) {
      config.path.split(toSplit, 2)
    }else{
      Array("" ,config.path)
    }

    val url = if (config.config.contains("url")){
      config.config.get("url").get
    }else{
      val format = config.config.getOrElse("implClass", fullFormat)

      ConnectMeta.options(DBMappingKey(format, _dbname)).get("url")
    }

    val dataSourceType = url.split(":")(1)
    val dbName = url.substring(url.lastIndexOf('/') + 1).takeWhile(_ != '?')

    SourceInfo(dataSourceType ,dbName ,_dbtable)
  }

}
