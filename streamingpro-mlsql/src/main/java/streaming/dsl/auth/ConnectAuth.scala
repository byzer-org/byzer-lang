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

import streaming.core.datasource.DataSourceRegistry
import streaming.dsl._
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge
import tech.mlsql.dsl.processor.AuthProcessListener

/**
  * Created by latincross on 12/27/2018.
  */
class ConnectAuth(authProcessListener: AuthProcessListener) extends MLSQLAuth with DslTool {
  val env = authProcessListener.listener.env().toMap

  def evaluate(value: String) = {
    TemplateMerge.merge(value, authProcessListener.listener.env().toMap)
  }

  override def auth(_ctx: Any): TableAuthResult = {
    val ctx = _ctx.asInstanceOf[SqlContext]
    var format = ""

    var option = Map[String, String]()
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          format = s.getText
          option += ("format" -> s.getText)

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

    TableAuthResult.empty()
  }
}

