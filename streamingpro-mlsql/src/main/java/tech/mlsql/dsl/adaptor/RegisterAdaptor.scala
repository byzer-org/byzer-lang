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

import java.util.UUID

import streaming.dsl.ScriptSQLExecListener
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 12/1/2018.
  */
class RegisterAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  def analyze(ctx: SqlContext): RegisterStatement = {
    var functionName = ""
    var format = ""
    var path = ""
    var option = Map[String, String]()

    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>

      ctx.getChild(tokenIndex) match {
        case s: FunctionNameContext =>
          functionName = s.getText
        case s: FormatContext =>
          format = s.getText
        case s: PathContext =>
          path = TemplateMerge.merge(cleanStr(s.getText), scriptSQLExecListener.env().toMap)
        case s: ExpressionContext =>
          option += (cleanStr(s.qualifiedName().getText) -> evaluate(getStrOrBlockStr(s)))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().qualifiedName().getText) -> evaluate(getStrOrBlockStr(s.expression())))
        case _ =>
      }
    }
    RegisterStatement(currentText(ctx), format, path, option, functionName)
  }

  override def parse(ctx: SqlContext): Unit = {
    val RegisterStatement(_, format, _path, option, functionName) = analyze(ctx)
    val resourceOwner = option.get("owner")
    var path = _path
    val alg = MLMapping.findAlg(format)
    if (!alg.skipPathPrefix) {
      path = resourceRealPath(scriptSQLExecListener, resourceOwner, path)
    }
    val sparkSession = scriptSQLExecListener.sparkSession
    val model = alg.load(sparkSession, path, option)
    val udf = alg.predict(sparkSession, model, functionName, option)
    if (udf != null) {
      scriptSQLExecListener.sparkSession.udf.register(functionName, udf)
    }
    val newdf = alg.explainModel(sparkSession, path, option)
    val tempTable = UUID.randomUUID().toString.replace("-", "")
    newdf.createOrReplaceTempView(tempTable)
    scriptSQLExecListener.setLastSelectTable(tempTable)
  }
}

case class RegisterStatement(raw: String, format: String, path: String, option: Map[String, String], functionName: String)


