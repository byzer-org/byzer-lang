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

import streaming.core.datasource.{DataAuthConfig, DataSourceRegistry, SourceInfo}
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge
import streaming.dsl.ScriptSQLExec
import streaming.log.{Logging, WowLog}
import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.dsl.processor.AuthProcessListener


/**
  * Created by allwefantasy on 11/9/2018.
  */
class SaveAuth(authProcessListener: AuthProcessListener) extends MLSQLAuth with DslTool with Logging with WowLog {
  val env = authProcessListener.listener.env().toMap

  def evaluate(value: String) = {
    TemplateMerge.merge(value, authProcessListener.listener.env().toMap)
  }

  override def auth(_ctx: Any): TableAuthResult = {
    val ctx = _ctx.asInstanceOf[SqlContext]
    var final_path = ""
    var format = ""
    var option = Map[String, String]()
    var tableName = ""
    var partitionByCol = Array[String]()

    val owner = option.get("owner")
    var path = ""

    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          format = s.getText

        case s: PathContext =>
          path = TemplateMerge.merge(cleanStr(s.getText), env)

        case s: TableNameContext =>
          tableName = s.getText
        case s: ColContext =>
          partitionByCol = cleanStr(s.getText).split(",")
        case s: ExpressionContext =>
          option += (cleanStr(s.qualifiedName().getText) -> evaluate(getStrOrBlockStr(s)))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().qualifiedName().getText) -> evaluate(getStrOrBlockStr(s.expression())))
        case _ =>
      }
    }

    val tableType = TableType.from(format) match {
      case Some(tt) => tt
      case None =>
        logWarning(wow_format(s"format ${format} is not supported yet by auth."))
        TableType.UNKNOW

    }

    val mLSQLTable = DataSourceRegistry.fetch(format, option).map { datasource =>
      val sourceInfo = datasource.asInstanceOf[ {def sourceInfo(config: DataAuthConfig): SourceInfo}].
        sourceInfo(DataAuthConfig(path, option))
      MLSQLTable(Some(sourceInfo.db), Some(sourceInfo.table), OperateType.SAVE, Some(sourceInfo.sourceType), tableType)
    } getOrElse {
      format match {
        case "hive" =>
          val Array(db, table) = final_path.split("\\.") match {
            case Array(db, table) => Array(db, table)
            case Array(table) => Array("default", table)
          }
          MLSQLTable(Some(db), Some(table), OperateType.SAVE, Some(format), TableType.HIVE)
        case _ =>
          val context = ScriptSQLExec.contextGetOrForTest()
          MLSQLTable(None, Some(resourceRealPath(context.execListener, owner, path)), OperateType.SAVE, Some(format), tableType)
      }
    }

    authProcessListener.addTable(mLSQLTable)
    TableAuthResult.empty()

  }
}
