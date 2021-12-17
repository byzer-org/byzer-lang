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

import _root_.streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.ScriptSQLExecListener
import streaming.dsl.template.TemplateMerge
import tech.mlsql.Stage
import tech.mlsql.common.utils.shell.ShellCommand
import tech.mlsql.dsl.scope.ParameterVisibility.ParameterVisibility
import tech.mlsql.dsl.scope.{ParameterVisibility, SetScope, SetVisibilityOption, SetVisibilityParameter}

import scala.collection.mutable

/**
 * Created by allwefantasy on 27/8/2017.
 */
class SetAdaptor(scriptSQLExecListener: ScriptSQLExecListener, stage: Stage.stage = Stage.physical) extends DslAdaptor {
  def analyze(ctx: SqlContext): SetStatement = {
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
    SetStatement(currentText(ctx), key, command, original_command, option, option.getOrElse("type", ""))
  }

  override def parse(ctx: SqlContext): Unit = {
    val SetStatement(_, key, command, original_command, option, mode) = analyze(ctx)
    var value = ""

    def evaluate(str: String) = {
      TemplateMerge.merge(str, scriptSQLExecListener.env().toMap)
    }

    def doRealJob(command: String): String = {
      val df = scriptSQLExecListener.sparkSession.sql(evaluate(command))

      new SelectAdaptor(scriptSQLExecListener).runtimeTableAuth(df)

      val resultHead = df.collect().headOption
      if (resultHead.isDefined) {
        resultHead.get.get(0).toString
      } else {
        ""
      }
    }

    var overwrite = true
    option.get("type") match {
      case Some("sql") =>
        // If we set mode compile, and then we should avoid the sql executed in
        // both preProcess and physical stage.
        val mode = SetMode.withName(option.get(SetMode.keyName).getOrElse(SetMode.runtime.toString))
        if (mode == SetMode.compile && stage == Stage.preProcess) {
          value = doRealJob(command)
        }
        if (mode == SetMode.runtime && stage == Stage.physical) {
          value = doRealJob(command)
        }
        // When the mode is compile, we should set  overwrite to false
        // to make sure the value (empty) will not overwrite the value computed
        // in stage preProcess
        if (mode == SetMode.compile && stage == Stage.physical) {
          overwrite = false
        }

      case Some("shell") =>
        value = ShellCommand.execCmd(evaluate(command)).trim
      case Some("conf") =>
        key match {
          case "spark.scheduler.pool" =>
            scriptSQLExecListener.sparkSession
              .sqlContext
              .sparkContext
              .setLocalProperty(key, original_command)
          case _ =>
            scriptSQLExecListener.sparkSession.sql(s""" set ${key} = ${original_command} """)
        }
      case Some("defaultParam") =>
        overwrite = false
        value = cleanBlockStr(cleanStr(command))
      case _ =>
        value = cleanBlockStr(cleanStr(command))
    }

    val scope = option.getOrElse(SetScope.NAME, "request")
    val visibility = option.getOrElse(SetVisibilityOption.NAME, "all").split(",")
    val visibilityParameter = visibility.foldLeft(mutable.Set[ParameterVisibility]())((x, y) => {
      y match {
        case "un_select" => x.add(ParameterVisibility.UN_SELECT)
        case _ =>
      }
      x
    })

    if (visibilityParameter.size == 0) {
      visibilityParameter.add(ParameterVisibility.ALL)
    }

    if (!overwrite) {
      if (!scriptSQLExecListener.env().contains(key)) {
        scriptSQLExecListener.addEnv(key, value)
        if (scope == "session"){
          scriptSQLExecListener.addSessionEnv(key, value,visibility.mkString(","))
        }         
        scriptSQLExecListener.addEnvVisibility(key, SetVisibilityParameter(value, visibilityParameter))
      }
    } else {
      scriptSQLExecListener.addEnv(key, value)
      if (scope == "session"){
        scriptSQLExecListener.addSessionEnv(key, value,visibility.mkString(","))
      }
      scriptSQLExecListener.addEnvVisibility(key, SetVisibilityParameter(value, visibilityParameter))
    }

    scriptSQLExecListener.env().view.foreach {
      case (k, v) =>

        val mergedValue = TemplateMerge.merge(v, scriptSQLExecListener.env().toMap)
        if (mergedValue != v) {
          scriptSQLExecListener.addEnv(k, mergedValue)
          if (scope == "session"){
            scriptSQLExecListener.addSessionEnv(key, value,visibility.mkString(","))
          }
        }
    }


    //unselect的变量可以TemplateMerge.merge到unselect的变量里，
    //unselect的变量不能TemplateMerge.merge到普通变量里，
    //普通变量可以TemplateMerge.merge到任何变量里

    val unSelect = scriptSQLExecListener.envVisibility
      .filter(_._2.scope.contains(ParameterVisibility.UN_SELECT))
      .map(f => (f._1, f._2.value)).toMap

    val select = scriptSQLExecListener.envVisibility
      .filter(!_._2.scope.contains(ParameterVisibility.UN_SELECT))
      .map(f => (f._1, f._2.value)).toMap


    scriptSQLExecListener.envVisibility().view.foreach {
      case (k, v) =>

        val scopedMergedValue = v.scope match {
          case ParameterVisibility.UN_SELECT => TemplateMerge.merge(v.value, select ++ unSelect)
          case _ => TemplateMerge.merge(v.value, select)
        }

        if (scopedMergedValue != v) {
          scriptSQLExecListener.addEnvVisibility(k, SetVisibilityParameter(scopedMergedValue, v.scope))
          v.scope match {
            case ParameterVisibility.UN_SELECT => unSelect.updated(k, scopedMergedValue)
            case _ => select.updated(k, scopedMergedValue)
          }
        }
    }

    scriptSQLExecListener.setLastSelectTable(null)
  }
}

object SetMode extends Enumeration {
  type mode = Value
  val compile = Value("compile")
  val runtime = Value("runtime")

  def keyName = "mode"

}

case class SetStatement(raw: String, key: String, command: String, original_command: String, option: Map[String, String], mode: String)