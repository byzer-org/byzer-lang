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
import streaming.dsl.ScriptSQLExecListener
import streaming.dsl.parser.DSLSQLLexer
import streaming.dsl.parser.DSLSQLParser.SqlContext
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 27/8/2017.
  */
class DropAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  def analyze(ctx: SqlContext): DropStatement = {
    val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input
    val start = ctx.start.getStartIndex()
    val stop = ctx.stop.getStopIndex()
    val interval = new Interval(start, stop)
    val originalText = input.getText(interval)
    val sql = TemplateMerge.merge(originalText, scriptSQLExecListener.env().toMap)
    DropStatement(originalText, sql)
  }

  override def parse(ctx: SqlContext): Unit = {
    val DropStatement(originalText, sql) = analyze(ctx)
    scriptSQLExecListener.sparkSession.sql(sql).count()
    scriptSQLExecListener.setLastSelectTable(null)
  }
}

case class DropStatement(raw: String, sql: String)
