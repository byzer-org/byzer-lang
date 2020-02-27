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

package streaming.dsl.mmlib.algs.param

import org.apache.spark.ml.param.{BooleanParam, Param}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.SparkSession
import streaming.dsl.mmlib.algs.SQLPythonFunc

/**
 * Created by allwefantasy on 14/9/2018.
 */
trait BaseParams extends WowParams {


  final val evaluateTable: Param[String] = new Param[String](this, "evaluateTable",
    "The table name of test dataset when tranning",
    (value: String) => true)


  final def getEvaluateTable: String = $(evaluateTable)

  def setEvaluateTable(value: String): this.type = set(evaluateTable, value)

  final val keepVersion: BooleanParam = new BooleanParam(this, "keepVersion", "If set true, then every time you run the " +
    "algorithm, it will generate a new directory to save the model.")

  setDefault(keepVersion -> true)

  final def getKeepVersion: Boolean = $(keepVersion)

  def setKeepVersion(value: Boolean): this.type = set(keepVersion, value)

  def getModelMetaData(spark: SparkSession, path: String) = {
    spark.read.parquet(SQLPythonFunc.getAlgMetalPath(path, getKeepVersion) + "/0")
  }
}

object BaseParams {
  def randomUID() = {
    Identifiable.randomUID(this.getClass.getName)
  }
}
