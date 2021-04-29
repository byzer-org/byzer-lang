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

package tech.mlsql.indexer.impl

import org.apache.spark.sql.catalyst.sqlgenerator.SQLDialect
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.{ByteType, DataType, StringType}
import tech.mlsql.common.ScalaReflect

import java.sql.Connection

/**
 * 21/03/2021 LiuYongheng(liuyonghengheng@gmail.com)
 */
class H2SQLDialect extends SQLDialect {

  override def canHandle(url: String): Boolean = url.toLowerCase().startsWith("jdbc:h2:")

  override def quote(name: String): String = {
    name
  }

  override def explainSQL(sql: String): String = s"EXPLAIN $sql"

  override def relation(alias: String, relation: LogicalRelation): String = {
    if (alias.isEmpty)
      ScalaReflect.fromInstance(relation.relation).field("jdbcOptions").invoke().asInstanceOf[JDBCOptions].tableOrQuery
    else alias
  }

  def relation2(alias: String, relation: LogicalRDD): String = {
    alias
  }

  override def dataTypeToSQL(dataType: DataType): String = {
    dataType match {
      case _ => dataType.sql
    }
  }

  override def maybeQuote(name: String): String = {
    name
  }

  override def getIndexes(conn: Connection, url: String, tableName: String): Set[String] = {
    Set()
  }

  override def getTableStat(conn: Connection, url: String, tableName: String): (Option[BigInt], Option[Long]) = {
    (None, None)
  }

  override def enableCanonicalize: Boolean = false
}
