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

package streaming.dsl.mmlib.algs

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.common.form.{Extra, FormParams, Text}

/**
  * Created by zhuml on 20/8/2018.
  */
class SQLWaterMarkInPlace(override val uid: String) extends SQLAlg with Functions with WowParams  {

  final val inputTable: Param[String]  = new Param[String] (this, "inputTable",
    FormParams.toJson(Text(
      name = "inputTable",
      value = "",
      extra = Extra(
        doc =
          """
            | The input table
          """,
        label = "the input table",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  final val eventTimeCol: Param[String]  = new Param[String] (this, "eventTimeCol",
    FormParams.toJson(Text(
      name = "eventTimeCol",
      value = "",
      extra = Extra(
        doc =
          """
            | Required. The name of the column that contains the event time of the row.
          """,
        label = "The name of the column that contains the event time of the row",
        options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )
  setDefault(eventTimeCol, "timestamp")

  final val delayThreshold: Param[String]  = new Param[String] (this, "delayThreshold",
    FormParams.toJson(Text(
      name = "delayThreshold",
      value = "",
      extra = Extra(
        doc =
          """
            | The minimum delay to wait to data to arrive late, relative to the latest record that has been processed
            |  in the form of an interval (e.g. "1 minute" or "5 hours"). NOTE: This should not be negative.
          """,
        label = "the delay threshold",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        "10 seconds"
      })
    )
    )
  )
  setDefault(delayThreshold, "10 seconds")

  def this() = this(Identifiable.randomUID("streaming.dsl.mmlib.algs.SQLWaterMarkInPlace"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    emptyDataFrame()(df)
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    val inputTable = params.getOrElse("inputTable", _path)
    val eventTimeCol = params.getOrElse("eventTimeCol", "timestamp")
    val delayThreshold = params.getOrElse("delayThreshold", "10 seconds")
    val df = spark.table(inputTable)
    df.withWatermark(eventTimeCol, delayThreshold).createOrReplaceTempView(inputTable)
    null
  }

  override def predict(spark: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }

  override def skipPathPrefix: Boolean = true

}