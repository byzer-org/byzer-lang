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

package streaming.dsl

import scala.collection.mutable

import org.antlr.v4.runtime.misc.Interval
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import streaming.dsl.parser.DSLSQLLexer
import streaming.dsl.parser.DSLSQLParser.SqlContext
import streaming.dsl.template.TemplateMerge


/**
  * Created by allwefantasy on 27/8/2017.
  */
class SelectAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  val ENABLE_RUNTIME_SELECT_AUTH = scriptSQLExecListener.sparkSession
    .sparkContext
    .getConf
    .getBoolean("spark.mlsql.enable.runtime.select.auth", false)

  override def parse(ctx: SqlContext): Unit = {
    val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input

    val start = ctx.start.getStartIndex()
    val stop = ctx.stop.getStopIndex()
    val interval = new Interval(start, stop)
    val originalText = input.getText(interval)

    val wowText = TemplateMerge.merge(originalText, scriptSQLExecListener.env().toMap)

    val chunks = wowText.split("\\s+")
    val tableName = chunks.last.replace(";", "")
    val sql = wowText.replaceAll(s"((?i)as)[\\s|\\n]+${tableName}", "")

    val df = scriptSQLExecListener.sparkSession.sql(sql)

//    ScriptSQLExec._tableAuth.auth(authListener.tables().tables.toList)
//    scriptSQLExecListener.getTableAuth.foreach(tableAuth => {
      var r = Array.empty[String]
      df.queryExecution.logical.map {
        case sp: UnresolvedRelation =>
          r +:= sp.tableIdentifier.unquotedString.toLowerCase
        case h: HiveTableRelation =>
          println(s"xxxxxxxx: ${h.tableMeta.identifier}")
        case _ =>
      }
      println(r.mkString(","))
      var tableAndCols = mutable.HashMap.empty[String, mutable.HashSet[String]]
      df.queryExecution.analyzed.map(lp => {
        lp.output.map(o => {
          val qualifier = o.qualifier.mkString(".")
          if (r.contains(o.qualifier.mkString("."))) {
            val value = tableAndCols.getOrElse(qualifier, mutable.HashSet.empty[String])
            value.add(o.name)
            tableAndCols.update(qualifier, value)
          }
        })
//      })
      tableAndCols.foreach(println)


        println("===================")
        r.distinct.map(t => {
          val tt = scriptSQLExecListener.sparkSession.catalog.getTable(t)
          println(tt)
          println(tt.tableType)
          println(tt.isTemporary)
        })
        println("===================")


    })

//    tableAuth.auth(authListener.tables().tables.toList)
    df.createOrReplaceTempView(tableName)
    scriptSQLExecListener.setLastSelectTable(tableName)
  }
}
