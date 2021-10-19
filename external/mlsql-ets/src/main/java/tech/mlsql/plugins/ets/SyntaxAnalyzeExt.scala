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

package tech.mlsql.plugins.ets

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.execution.MLSQLAuthParser
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.param.WowParams
import streaming.log.WowLog
import tech.mlsql.common.form.{Extra, FormParams, KV, Select, Text}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

/**
  * 09/08/2021 hellozepp(lisheng.zhanglin@163.com)
  */
class SyntaxAnalyzeExt(override val uid: String) extends SQLAlg with WowParams with Logging with WowLog with ETAuth {
  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.SyntaxAnalyzeExt"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    params.get(sql.name)
      .map(s => s.trim)
      .map(s => if (s != "" && s.last.equals(';')) s.dropRight(1) else s)
      .filter(_ != "")
      .map { s =>
        params.getOrElse(action.name, getOrDefault(action)) match {
          case "extractTables" =>
            import df.sparkSession.implicits._
            MLSQLAuthParser.filterTables(s, context.execListener.sparkSession).map(_.table).toDF("tableName")
        }
      }.getOrElse {
      df.sparkSession.emptyDataFrame
    }
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support predict function.")
  }

  override def modelType: ModelType = ProcessType

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = params.getOrElse(action.name, getOrDefault(action)) match {
      case "extractTables" =>
        MLSQLTable(
          Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
          Option("__syntax_analyze_operator__"),
          OperateType.SELECT,
          Option("select"),
          TableType.SYSTEM)
      case _ =>
        throw new NoSuchElementException("Failed to execute SyntaxAnalyzeExt, unsupported action")
    }

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getTableAuth match {
      case Some(tableAuth) =>
        tableAuth.auth(List(vtable))
      case None =>
        List(TableAuthResult(granted = true, ""))
    }
  }

  final val sql: Param[String]  = new Param[String] (this, "sql",
    FormParams.toJson(Text(
      name = "sql",
      value = "",
      extra = Extra(
        doc =
          """
            | Required. SQL to be analyzed
            | e.g. sql = "select * from table"
          """,
        label = "sql",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )
  setDefault(sql, "")

  final val action: Param[String]  = new Param[String] (this, "action",
    FormParams.toJson(Select(
      name = "action",
      values = List(),
      extra = Extra(
        doc =
          """
            | Action for syntax analysis
            | Optional parameter: extractTables
            | Notice: Currently, the only supported action is `extractTables`,
            | and other parameters of the action are under construction.
            | e.g. action = "extractTables"
          """,
        label = "action for syntax analysis",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        List(KV(Option("action"), Option("extractTables")))
      })
    )
    )
  )
  setDefault(action, "extractTables")

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }

  override def doc: Doc = Doc(HtmlDoc,
    """
      | SyntaxAnalyzeExt is used to parse the SQL grammar in the statement, please
      | check the codeExample to see how to use it.
      |
      | Use "load modelParams.`SyntaxAnalyzeExt` as output;"
      | to check the available parameters;
      |
      | Use "load modelExample.`SyntaxAnalyzeExt` as output;"
      | get example.
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode,
    """
      | Execute SyntaxAnalyzeExt to parse a nested SQL. As follows:
      | ```sql
      | run command as SyntaxAnalyzeExt.`` where
      | action = "extractTables" and sql='''
      | select * from (select * from table1 as c) as d left join table2 as e on d.id=e.id
      | ''' as extractedTables;
      |
      | select * from extractedTables as output;
      | ```
      |
      | then the result is:
      | ```
      | +----------+
      | |tableName |
      | +----------+
      | |table1    |
      | |table2    |
      | +----------+
      | ```
      |""".stripMargin)
}

