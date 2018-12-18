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

package org.apache.spark.sql.jdbc

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}

/**
  * Created by allwefantasy on 26/4/2018.
  */
object DataFrameWriterExtensions {

  implicit class Upsert(w: DataFrameWriter[Row]) {
    def upsert(idCol: Option[String], jdbcOptions: JDBCOptions, df: DataFrame): Unit = {
      val idColumn = idCol.map(f => df.schema.filter(s => f.split(",").contains(s.name)))
      val url = jdbcOptions.url
      val table = jdbcOptions.table
      val modeF = w.getClass.getDeclaredField("mode")
      modeF.setAccessible(true)
      val mode = modeF.get(w).asInstanceOf[SaveMode]
      val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
      val isCaseSensitive = df.sqlContext.conf.caseSensitiveAnalysis

      try {
        var tableExists = JdbcUtils.tableExists(conn, new JDBCOptions(url, table, Map.empty))

        if (mode == SaveMode.Ignore && tableExists) {
          return
        }

        if (mode == SaveMode.ErrorIfExists && tableExists) {
          sys.error(s"Table $table already exists.")
        }

        if (mode == SaveMode.Overwrite && tableExists) {
          JdbcUtils.dropTable(conn, table)
          tableExists = false
        }

        // Create the table if the table didn't exist.
        if (!tableExists) {
          val schema = JdbcUtils.schemaString(df, url, jdbcOptions.createTableColumnTypes)
          val dialect = JdbcDialects.get(url)
          val pk = idColumn.map { f =>
            val key = f.map(c => s"${dialect.quoteIdentifier(c.name)}").mkString(",")
            s", primary key(${key})"
          }.getOrElse("")
          val sql = s"CREATE TABLE $table ( $schema $pk )"
          val statement = conn.createStatement
          try {
            statement.executeUpdate(sql)
          } finally {
            statement.close()
          }
        }
      } finally {
        conn.close()
      }

      //todo: make this a single method
      idColumn match {
        case Some(id) => UpsertUtils.upsert(df, idColumn, jdbcOptions, isCaseSensitive)
        case None => JdbcUtils.saveTable(df, Some(df.schema), isCaseSensitive, options = jdbcOptions)
      }

    }

  }

}
