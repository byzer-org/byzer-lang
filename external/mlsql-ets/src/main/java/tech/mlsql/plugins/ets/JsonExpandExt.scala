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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.{Code, Doc, HtmlDoc, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.param.WowParams
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import org.apache.spark.sql.functions._
import streaming.dsl.auth.TableAuthResult
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.plugins.ets.JsonExpandExt.{inputCol, samplingRatio}

object JsonExpandExt {
  final val inputCol: Param[String] = new Param[String](parent = "JsonExpandExt"
    , name = "inputCol"
    , doc = """Required. Json column to be expanded
        |e.g. WHERE inputCol = col_1 """.stripMargin
  )

  final val samplingRatio: Param[String] = new Param[String](parent = "JsonExpandExt"
    , name = "samplingRatio"
    , doc = """Optional. SamplingRatio used by Spark to infer schema from json, 1.0 by default.
        |e.g. WHERE sampleRatio = "0.2" """.stripMargin)

}

class JsonExpandExt (override val uid: String) extends SQLAlg with WowParams with Logging with WowLog with ETAuth {
  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.JsonExpandExt"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    // Parameter checking
    require(params.contains(inputCol.name),
      "inputCol is required. e.g. inputCol = col_1, where col_1 is the json column ")

    val spark = df.sparkSession
    import spark.implicits._
    params.get(inputCol.name) match {
      case Some(col) =>
        // spark.sql.AnalysisException thrown if col name was wrong.
        val colValueDs = df.select( col ).as[String]

        val ratio = params.getOrElse(samplingRatio.name, "1.0").toDouble
        logInfo( format(s"samplingRatio ${ratio}") )

        // Infers schema from json
        val schema = df.sparkSession.read
          .option("samplingRatio", ratio )
          .json(colValueDs).schema

        // Throws RuntimeException if json is invalid
        if( schema.exists(_.name == spark.sessionState.conf.columnNameOfCorruptRecord ) )
          throw new RuntimeException(s"Corrupted JSON in column ${col}")

        val expandedCol = from_json( df( col ), schema).as("expanded")
        df.select(expandedCol).select("expanded.*")

      case None => df    // Should not happen

    }
  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
//    ScriptSQLExec.contextGetOrForTest.execListener.sparkSession
    //MLSQLAuthParser.filterTables()
    List()
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {}

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String])
    : UserDefinedFunction = ???

  override def doc: Doc = Doc(HtmlDoc,
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
      |SELECT '{"key": "value", "key_2":"value_2"}' AS col_1 AS table_1;
      |
      |##  Expand json from col_1, please note that there are 2 columns
      |##  in the result set
      |run table_1 as JsonExpandExt.`` where inputCol="col_1" AND samplingRatio = "0.5" as A2;
      |```
      |output:
      |```
      |+---------------+
      |key    |key_2   |
      |+---------------+
      |value  |value_2 |
      |+---------------+
      |```
      |""".stripMargin)


}
