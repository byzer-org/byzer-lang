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

import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge
import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.dsl.processor.AuthProcessListener


/**
  * Created by allwefantasy on 11/9/2018.
  */
class SetAuth(authProcessListener: AuthProcessListener) extends MLSQLAuth with DslTool {
  val env = authProcessListener.listener.env().toMap

  def evaluate(value: String) = {
    TemplateMerge.merge(value, authProcessListener.listener.env().toMap)
  }

  override def auth(_ctx: Any): TableAuthResult = {
    val ctx = _ctx.asInstanceOf[SqlContext]
    var key = ""
    var value = ""
    var command = ""
    var original_command = ""
    var option = Map[String, String]()
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>

      ctx.getChild(tokenIndex) match {
        case s: SetKeyContext =>
          key = s.getText
        case s: SetValueContext =>
          original_command = s.getText
          if (s.quotedIdentifier() != null && s.quotedIdentifier().BACKQUOTED_IDENTIFIER() != null) {
            command = cleanStr(s.getText)
          } else if (s.qualifiedName() != null && s.qualifiedName().identifier() != null) {
            command = cleanStr(s.getText)
          }
          else {
            command = original_command
          }
        case s: ExpressionContext =>
          option += (cleanStr(s.qualifiedName().getText) -> getStrOrBlockStr(s))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().qualifiedName().getText) -> getStrOrBlockStr(s.expression()))
        case _ =>
      }
    }

    val table = MLSQLTable(Some(DB_DEFAULT.MLSQL_SYSTEM.toString), Some(option.get("type").getOrElse("string")), OperateType.SET, None, TableType.GRAMMAR)
    authProcessListener.addTable(table)
    TableAuthResult.empty()
  }
}
