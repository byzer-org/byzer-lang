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

package streaming.dsl.auth

import streaming.dsl.auth.OperateType.OperateType

/**
  * Created by allwefantasy on 11/9/2018.
  */
case class MLSQLTable(
                       db: Option[String],
                       table: Option[String],
                       columns: Option[Set[String]],
                       operateType: OperateType,
                       sourceType: Option[String],
                       tableType: TableTypeMeta) {
  def tableIdentifier: String = {
    if (db.isDefined && table.isDefined) {
      s"${db.get}.${table.get}"
    } else if (!db.isDefined && table.isDefined) {
      table.get
    } else {
      ""
    }
  }
}

object MLSQLTable {
  def apply(db: Option[String],
            table: Option[String],
            operateType: OperateType,
            sourceType: Option[String],
            tableType: TableTypeMeta): MLSQLTable =
    new MLSQLTable(db, table, None, operateType, sourceType, tableType)
}

case class MLSQLTableSet(tables: Seq[MLSQLTable])

case class TableTypeMeta(name: String, includes: Set[String])

case class TableAuthResult(granted: Boolean, msg: String)

object TableAuthResult {
  def empty() = {
    TableAuthResult(false, "")
  }
}

object DB_DEFAULT extends Enumeration {
  type DB_DEFAULT = Value
  val MLSQL_SYSTEM = Value("mlsql_system")
}

object OperateType extends Enumeration {
  type OperateType = Value
  val SAVE = Value("save")
  val LOAD = Value("load")
  val DIRECT_QUERY = Value("directQuery")
  val CREATE = Value("create")
  val DROP = Value("drop")
  val INSERT = Value("insert")
  val UPDATE = Value("update")
  val SELECT = Value("select")
  val SET = Value("set")
  val EMPTY = Value("empty")

  def toList = {
    List(SAVE.toString, LOAD.toString, DIRECT_QUERY.toString,
      CREATE.toString, DROP.toString, INSERT.toString, UPDATE.toString,
      SELECT.toString, SET.toString, EMPTY.toString)
  }
}


object TableType {
  val HIVE = TableTypeMeta("hive", Set("hive"))
  val CUSTOME = TableTypeMeta("custom", Set("custom"))
  val BINLOG = TableTypeMeta("binlog", Set("binlog"))
  val HBASE = TableTypeMeta("hbase", Set("hbase"))
  val HDFS = TableTypeMeta("hdfs", Set("parquet", "binlogRate", "json", "csv", "image", "text", "xml", "excel", "libsvm", "delta", "rate", "streamParquet"))
  val HTTP = TableTypeMeta("http", Set("http"))
  val JDBC = TableTypeMeta("jdbc", Set("jdbc", "streamJDBC"))
  val ES = TableTypeMeta("es", Set("es"))
  val REDIS = TableTypeMeta("redis", Set("redis"))
  val KAFKA = TableTypeMeta("kafka", Set("kafka", "kafka8", "kafka9", "adHocKafka"))
  val SOCKET = TableTypeMeta("socket", Set("socket"))
  val MONGO = TableTypeMeta("mongo", Set("mongo"))
  val SOLR = TableTypeMeta("solr", Set("solr", "streamSolr"))
  val TEMP = TableTypeMeta("temp", Set("temp", "jsonStr", "script", "csvStr", "mockStream", "console", "webConsole"))
  val API = TableTypeMeta("api", Set("mlsqlAPI", "mlsqlConf"))
  val WEB = TableTypeMeta("web", Set("crawlersql"))
  val GRAMMAR = TableTypeMeta("grammar", Set("grammar"))
  val SYSTEM = TableTypeMeta("system", Set("_mlsql_", "model", "modelList", "modelParams", "modelExample", "modelExplain"))
  val UNKNOW = TableTypeMeta("unknow", Set("unknow"))

  def from(str: String) = {
    List(BINLOG, UNKNOW, KAFKA, SOCKET, REDIS, HIVE, HBASE, HDFS, HTTP, JDBC, ES, MONGO, SOLR, TEMP, API, WEB, GRAMMAR, SYSTEM, CUSTOME).filter(f => f.includes.contains(str)).headOption
  }

  def toList = {
    List(BINLOG, UNKNOW, KAFKA, SOCKET, REDIS, HIVE, HBASE, HDFS, HTTP, JDBC, ES, MONGO, SOLR, TEMP, API, WEB, GRAMMAR, SYSTEM, CUSTOME).map(f => f.name)
  }
}
