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

import streaming.core.datasource.DataSourceRegistry
import streaming.dsl.{ConnectMeta, DBMappingKey, ScriptSQLExecListener}
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 27/8/2017.
  */
class ConnectAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  def analyze(ctx: SqlContext): ConnectStatement = {
    var option = Map[String, String]()
    var format = ""

    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          format = s.getText
          option += ("format" -> format)

        case s: ExpressionContext =>
          option += (cleanStr(s.qualifiedName().getText) -> evaluate(getStrOrBlockStr(s)))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().qualifiedName().getText) -> evaluate(getStrOrBlockStr(s.expression())))
        case _ =>

      }
    }
    ConnectStatement(currentText(ctx), format, option)
  }

  override def parse(ctx: SqlContext): Unit = {

    var option = Map[String, String]()
    var format = ""

    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          format = s.getText
          option += ("format" -> format)

        case s: ExpressionContext =>
          option += (cleanStr(s.qualifiedName().getText) -> evaluate(getStrOrBlockStr(s)))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().qualifiedName().getText) -> evaluate(getStrOrBlockStr(s.expression())))
        case s: DbContext =>
          DataSourceRegistry.findAllNames(format).map { names =>
            names.foreach { name => ConnectMeta.options(DBMappingKey(name, s.getText), option) }
          }.getOrElse {
            ConnectMeta.options(DBMappingKey(format, s.getText), option)
          }
        case _ =>

      }
    }
    scriptSQLExecListener.setLastSelectTable(null)
  }
}

case class ConnectStatement(raw: String, format: String, option: Map[String, String])
