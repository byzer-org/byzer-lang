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

import java.util.UUID

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.OperateType.SELECT
import streaming.dsl.auth.TableType.SYSTEM
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, TableAuthResult}
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.param.WowParams
import streaming.log.WowLog
import tech.mlsql.common.form.{Extra, FormParams, Text}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

class JsonExpandExt(override val uid: String) extends SQLAlg with WowParams with Logging with WowLog with ETAuth {
  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.JsonExpandExt"))

  final val inputCol: Param[String] = new Param[String](parent = this
    , name = "inputCol"
    , doc = FormParams.toJson(Text(
      name = "inputCol"
      , value = ""
      , extra = Extra(
        doc = "Json column to be expanded"
        , label = "inputCol"
        , options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )
      )
    )
    )
  )


  final val samplingRatio: Param[String] = new Param[String](parent = this
    , name = "samplingRatio"
    , doc = FormParams.toJson(Text(
      name = "samplingRatio"
      , value = ""
      , extra = Extra(
        doc = "SamplingRatio used by Spark to infer schema from json"
        , label = "samplingRatio"
        , options = Map(
          "valueType" -> "double",
          "required" -> "false",
          "derivedType" -> "NONE"
        )
      )
    )
    )
  )
  setDefault(samplingRatio, "1.0")
  final val structColumn: Param[String] = new Param[String](parent = this
    , name = "structColumn"
    , doc = FormParams.toJson( Text(
      name = "structColumn"
      , value = "false"
      , extra = Extra(
        doc = "turning the json string into Struct object"
        , label = "structColumn"
        , options = Map (
          "valueType" -> "boolean"
          , "required" -> "false"
          , "derivedType" -> "NONE"
        )
      )
    ) )
  )
  setDefault(structColumn, "false")

  /**
   * Expands a json column to multiple columns. Json column name is addressed by parameter inputCol's value
   *
   * @param df     Input dataframe.
   * @param path   Not used
   * @param params Input parameters, must contain inputCol
   * @return The dataframe with non-json columns and json expanded columns
   * @throws        IllegalArgumentException if inputCol is not present in params
   */
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    // Parameter checking
    require(params.contains(inputCol.name),
      "inputCol is required. e.g. inputCol = col_1, where col_1 is the json column ")

    val spark = df.sparkSession
    import spark.implicits._
    params.get(inputCol.name) match {
      case Some(col) =>
        val ratio = params.getOrElse(samplingRatio.name, "1.0").toDouble
        logInfo(format(s"samplingRatio ${ratio}"))

        // spark.sql.AnalysisException thrown if col name was wrong.
        val colValueDs = df.select(col).as[String]
        // Infers schema from json
        val schema = spark.read
          .option("samplingRatio", ratio)
          .json(colValueDs).schema

        // Throws RuntimeException if json is invalid
        if (schema.exists(_.name == spark.sessionState.conf.columnNameOfCorruptRecord))
          throw new RuntimeException(s"Corrupted JSON in column ${col}")

        if (params.getOrElse("structColumn", "false").toBoolean) {
          val jsonName = params(inputCol.name)
          val tempName = UUID.randomUUID().toString.replaceAll("-","")
          val tempDf = df.withColumnRenamed(jsonName,tempName)
          return tempDf.select(tempDf("*"), from_json(tempDf(tempName), schema).as(jsonName)).drop(tempName)
        }

        // Expand json and return a new DataFrame including non-json columns
        val expandedCols = schema.fields.map(_.name)
        val originalCols = df.schema.map(_.name)
        df.select(df("*"), json_tuple(df(col), expandedCols: _*))
          .toDF((originalCols ++ expandedCols): _*)
          .drop(col)

      case None => df // Should not happen

    }
  }

  /**
   * Wraps this plugin as a {@link streaming.dsl.auth.MLSQLTable} and delegates authorization to implementation of
   * {@link streaming.dsl.auth.TableAuth}
   *
   * @param etMethod
   * @param path
   * @param params
   * @return
   */
  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    // Parameter checking
    require(params.contains(inputCol.name),
      "inputCol is required. e.g. inputCol = col_1, where col_1 is the json column ")

    val table = MLSQLTable(db = Some(DB_DEFAULT.MLSQL_SYSTEM.toString)
      , table = Some("__json_expand_operator__")
      , columns = None
      , operateType = SELECT
      , sourceType = Some("select")
      , tableType = SYSTEM
    )

    val context = ScriptSQLExec.contextGetOrForTest()

    context.execListener.getTableAuth match {
      case Some(tableAuth) => tableAuth.auth(List(table))
      case None => List(TableAuthResult(granted = true, ""))
    }

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {}

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String])
  : UserDefinedFunction = ???

  override def doc: Doc = Doc(MarkDownDoc,
    """
      | JsonExpandExt is used to expand json strings, please
      | see the codeExample to learn its usage.
      |
      | Use "load modelExample.`JsonExpandExt` as output;"
      | to see the codeExample.
    """.stripMargin)

  override def codeExample: Code = Code(SQLCode,
    """
      |```sql
      |## Generate a table named "table_1" with one json column "col_1"
      |SELECT '{"city": "hangzhou", "country":"China"}' AS col_1 AS table_1;
      |
      |##  Expand json from col_1, please note that there are 2 columns
      |##  in the result set
      |run table_1 as JsonExpandExt.`` where inputCol="col_1" AND samplingRatio = "0.5" as A2;
      |```
      |output:
      |```
      |+-----------------+
      |city     |country |
      |+-----------------+
      |hangzhou |China   |
      |+-----------------+
      |```
      |""".stripMargin)

  /**
   * Explanation to each parameter's name and doc
   *
   * @param sparkSession The SparkSQL Session
   * @return
   */
  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }

  override def modelType: ModelType = ProcessType

}