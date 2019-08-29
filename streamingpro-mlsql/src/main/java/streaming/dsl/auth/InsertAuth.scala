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

import org.antlr.v4.runtime.misc.Interval
import org.apache.spark.sql.execution.MLSQLAuthParser
import streaming.dsl.parser.DSLSQLLexer
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge
import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.dsl.processor.AuthProcessListener


/**
  * Created by allwefantasy on 11/9/2018.
  */
class InsertAuth(authProcessListener: AuthProcessListener) extends MLSQLAuth with DslTool {
  val env = authProcessListener.listener.env().toMap

  def evaluate(value: String) = {
    TemplateMerge.merge(value, authProcessListener.listener.env().toMap)
  }

  override def auth(_ctx: Any): TableAuthResult = {
    val ctx = _ctx.asInstanceOf[SqlContext]
    val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input

    val start = ctx.start.getStartIndex()
    val stop = ctx.stop.getStopIndex()
    val interval = new Interval(start, stop)
    val originalText = input.getText(interval)

    val sql = TemplateMerge.merge(originalText, env)



    val tableRefs = MLSQLAuthParser.filterTables(sql, authProcessListener.listener.sparkSession)

    val tables = tableRefs.foreach { f =>
      f.database match {
        case Some(db) =>
          val operateType = f.operator match {
            case Some(operator) => OperateType.INSERT
            case None => OperateType.SELECT
          }
          val exists = authProcessListener.withDBs.filter(m => f.table == m.table.get && db == m.db.get  && operateType == m.operateType).size > 0
          if (!exists) {
            authProcessListener.addTable(MLSQLTable(Some(db), Some(f.table) ,operateType , None, TableType.HIVE))
          }
        case None =>
          val exists = authProcessListener.withoutDBs.filter(m => f.table == m.table.get).size > 0
          if (!exists) {
            val operateType = f.operator match {
              case Some(operator) => OperateType.INSERT
              case None => OperateType.SELECT
            }
            authProcessListener.addTable(MLSQLTable(None, Some(f.table) ,operateType, None, TableType.TEMP))
          }
      }
    }
    TableAuthResult.empty()

  }
}
