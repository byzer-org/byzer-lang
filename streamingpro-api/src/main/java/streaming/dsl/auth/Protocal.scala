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
case class MLSQLTable(db: Option[String], table: Option[String], operateType: OperateType, sourceType: Option[String], tableType: TableTypeMeta)

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
  val CREATE = Value("create")
  val DROP = Value("drop")
  val INSERT = Value("insert")
  val SELECT = Value("select")
  val SET = Value("set")
  val EMPTY = Value("empty")
}

object TableType {
  val HIVE = TableTypeMeta("hive", Set("hive"))
  val HBASE = TableTypeMeta("hbase", Set("hbase"))
  val HDFS = TableTypeMeta("hdfs", Set("parquet", "json", "csv", "image", "text", "xml"))
  val HTTP = TableTypeMeta("http", Set("http"))
  val JDBC = TableTypeMeta("jdbc", Set("jdbc"))
  val ES = TableTypeMeta("es", Set("es"))
  val MONGO = TableTypeMeta("mongo", Set("mongo"))
  val SOLR = TableTypeMeta("solr", Set("solr"))
  val TEMP = TableTypeMeta("temp", Set("temp", "jsonStr", "script", "csvStr"))
  val API = TableTypeMeta("api", Set("mlsqlAPI", "mlsqlConf"))
  val WEB = TableTypeMeta("web", Set("crawlersql"))
  val GRAMMAR = TableTypeMeta("grammar", Set("grammar"))

  def from(str: String) = {
    List(HIVE, HBASE, HDFS, HTTP, JDBC, ES, MONGO, SOLR, TEMP, API, WEB, GRAMMAR).filter(f => f.includes.contains(str)).headOption
  }

  def toList = {
    List(HIVE, HBASE, HDFS, HTTP, JDBC, ES, MONGO, SOLR, TEMP, API, WEB, GRAMMAR).flatMap(f => f.includes.toSeq)
  }
}



