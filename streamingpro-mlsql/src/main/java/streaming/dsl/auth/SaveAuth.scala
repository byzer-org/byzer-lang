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
import streaming.dsl.{AuthProcessListener, DslTool}


/**
  * Created by allwefantasy on 11/9/2018.
  */
class SaveAuth(authProcessListener: AuthProcessListener) extends MLSQLAuth with DslTool {
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

    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          format = s.getText
          format match {
            case "hive" =>
            case _ =>
              format = s.getText
          }


        case s: PathContext =>
          format match {
            case "hive" | "kafka8" | "kafka9" | "hbase" | "redis" | "es" | "jdbc" =>
              final_path = cleanStr(s.getText)
            case "parquet" | "json" | "csv" | "orc" =>
              final_path = withPathPrefix(authProcessListener.listener.pathPrefix(owner), cleanStr(s.getText))
            case _ =>
              final_path = cleanStr(s.getText)
          }

          final_path = TemplateMerge.merge(final_path, env)

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

    val mLSQLTable = DataSourceRegistry.fetch(format, option).map { datasource =>
      val sourceInfo = datasource.asInstanceOf[ {def sourceInfo(config: DataAuthConfig): SourceInfo}].
        sourceInfo(DataAuthConfig(final_path, option))
      MLSQLTable(Some(sourceInfo.db), Some(sourceInfo.table), OperateType.SAVE , Some(sourceInfo.sourceType), TableType.from(format).get)
    } getOrElse {
      format match {
        case "hive" =>
          val Array(db, table) = final_path.split("\\.")
          MLSQLTable(Some(db), Some(table), OperateType.SAVE ,Some(format) , TableType.HIVE)
        case _ =>
          MLSQLTable(None, Some(final_path), OperateType.SAVE ,Some(format) , TableType.from(format).get)
      }
    }

    authProcessListener.addTable(mLSQLTable)
    TableAuthResult.empty()

  }
}
