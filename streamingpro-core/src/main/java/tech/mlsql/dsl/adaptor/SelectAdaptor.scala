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

package tech.mlsql.dsl.adaptor

import org.antlr.v4.runtime.misc.Interval
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.MLSQLDFParser
import streaming.dsl.auth.{MLSQLTable, OperateType, TableType}
import streaming.dsl.parser.DSLSQLLexer
import streaming.dsl.parser.DSLSQLParser.SqlContext
import streaming.dsl.template.TemplateMerge
import streaming.dsl.{ForContext, IfContext, ScriptSQLExecListener}
import tech.mlsql.dsl.scope.ParameterScope
import tech.mlsql.sql.MLSQLSparkConf


/**
 * Created by allwefantasy on 27/8/2017.
 */
class SelectAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor with DslTool {

  def analyze(ctx: SqlContext): SelectStatement = {
    val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input

    val start = ctx.start.getStartIndex()
    val stop = ctx.stop.getStopIndex()
    val interval = new Interval(start, stop)
    val originalText = input.getText(interval)

    val envScope = scriptSQLExecListener.envScope
      .filter(!_._2.scope.contains(ParameterScope.UN_SELECT))
      .mapValues(_.value)
      .toMap

    val wowText = TemplateMerge.merge(originalText, envScope)

    val chunks = wowText.split("\\s+")
    val tableName = chunks.last.replace(";", "")
    val sql = try {
      wowText.replaceAll(s"((?i)as)[\\s|\\n]+${tableName}\\s*\\n*$$", "")
    } catch {
      case e: Exception =>
        wowText.split("(?i)as").dropRight(1).mkString("as")
    }

    SelectStatement(originalText, sql, tableName)

  }

  override def parse(ctx: SqlContext): Unit = {
    
    val SelectStatement(originalText, sql, tableName) = analyze(ctx)

    val df = scriptSQLExecListener.sparkSession.sql(sql)

    runtimeTableAuth(df)

    df.createOrReplaceTempView(tableName)
    scriptSQLExecListener.setLastSelectTable(tableName)
  }

  def runtimeTableAuth(df: DataFrame): Unit = {
    //enable runtime select auth
    if (MLSQLSparkConf.runtimeSelectAuth) {
      scriptSQLExecListener.getTableAuth.foreach(tableAuth => {

        val tableAndCols = MLSQLDFParser.extractTableWithColumns(df)
        var mlsqlTables = List.empty[MLSQLTable]

        tableAndCols.foreach {
          case (dbTable, cols) =>
            val Array(dbName, tableName) = dbTable.split("\\.", 2)
            mlsqlTables ::= MLSQLTable(Option(dbName), Option(tableName), Option(cols.toSet), OperateType.SELECT, None, TableType.HIVE)
        }

        tableAuth.auth(mlsqlTables)
      })
    }

  }
}

case class SelectStatement(raw: String, sql: String, tableName: String)