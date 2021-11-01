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

import MetaConst._
import org.apache.spark.sql.{DataFrame, MLSQLUtils, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, DoubleType}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.Param
import streaming.dsl.mmlib.{Code, Doc, HtmlDoc, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.feature.DoubleFeature
import streaming.dsl.mmlib.algs.meta.ScaleMeta
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.form.{Extra, FormParams, KV, Select, Text}

/**
 * Created by allwefantasy on 24/5/2018.
 */
class SQLScalerInPlace(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseClassification {
  def this() = this(BaseParams.randomUID())

  def internal_train(df: DataFrame, params: Map[String, String]) = {
    val path = params("path")
    val metaPath = getMetaPath(path)
    saveTraningParams(df.sparkSession, params, metaPath)
    val inputCols = params.getOrElse(this.inputCols.name, "").split(",")
    val scaleMethod = params.getOrElse(this.scaleMethod.name, "log2")
    val removeOutlierValue = params.getOrElse(this.removeOutlierValue.name, "false").toBoolean
    require(!inputCols.isEmpty, "inputCols is required when use SQLScalerInPlace")
    var newDF = df
    if (removeOutlierValue) {
      newDF = DoubleFeature.killOutlierValue(df, metaPath, inputCols)
    }
    newDF = DoubleFeature.scale(df, metaPath, inputCols, scaleMethod, params)
    newDF
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val newDF = internal_train(df, params + ("path" -> path))
    newDF.write.mode(SaveMode.Overwrite).parquet(getDataPath(path))
    emptyDataFrame()(df)
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    //load train params
    val path = getMetaPath(_path)
    val (trainParams, df) = getTranningParams(spark, path)
    val inputCols = trainParams.getOrElse("inputCols", "").split(",").toSeq
    val scaleMethod = trainParams.getOrElse("scaleMethod", "log2")
    val removeOutlierValue = trainParams.getOrElse("removeOutlierValue", "false").toBoolean

    val scaleFunc = scaleMethod match {
      case "min-max" =>
        DoubleFeature.getMinMaxModelForPredict(spark, inputCols, path, trainParams)
      case "log2" =>
        DoubleFeature.baseRescaleFunc((a) => Math.log(a))

      case "logn" =>
        DoubleFeature.baseRescaleFunc((a) => Math.log1p(a))

      case "log10" =>
        DoubleFeature.baseRescaleFunc((a) => Math.log10(a))
      case "sqrt" =>
        DoubleFeature.baseRescaleFunc((a) => Math.sqrt(a))

      case "abs" =>
        DoubleFeature.baseRescaleFunc((a) => Math.abs(a))
      case _ =>
        DoubleFeature.baseRescaleFunc((a) => Math.log(a))
    }

    var meta = ScaleMeta(trainParams, null, scaleFunc)

    if (removeOutlierValue) {
      val removeOutlierValueFunc = DoubleFeature.getModelOutlierValueForPredict(spark, path, inputCols, trainParams)
      meta = meta.copy(removeOutlierValueFunc = removeOutlierValueFunc)
    }
    meta
  }
  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val meta = _model.asInstanceOf[ScaleMeta]
    val removeOutlierValue = meta.trainParams.getOrElse("removeOutlierValue", "false").toBoolean
    val inputCols = meta.trainParams.getOrElse("inputCols", "").split(",").toSeq
    val f = (values: Seq[Double]) => {
      val newValues = if (removeOutlierValue) {
        values.zipWithIndex.map { v =>
          meta.removeOutlierValueFunc(v._1, inputCols(v._2))
        }
      } else values
      meta.scaleFunc(Vectors.dense(newValues.toArray)).toArray
    }
    MLSQLUtils.createUserDefinedFunction(f, ArrayType(DoubleType), Some(Seq(ArrayType(DoubleType))))
  }

  override def doc: Doc = Doc(HtmlDoc,
    """
      | <a href="https://en.wikipedia.org/wiki/Feature_scaling">Feature scaling</a>
      |
      | Feature scaling is a method used to normalize the range of independent variables or features of data.
      | In data processing, it is also known as data normalization and
      | is generally performed during the data preprocessing step.
      |
      | Use "load modelParams.`ScalerInPlace` as output;"
      |
      | to check the available hyper parameters;
      |
      |
    """.stripMargin)

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |set jsonStr='''
      |{"a":1,    "b":100, "label":0.0},
      |{"a":100,  "b":100, "label":1.0}
      |{"a":1000, "b":100, "label":0.0}
      |{"a":10,   "b":100, "label":0.0}
      |{"a":1,    "b":100, "label":1.0}
      |''';
      |load jsonStr.`jsonStr` as data;
      |
      |train data as ScalerInPlace.`/tmp/scaler`
      |where inputCols="a,b"
      |and scaleMethod="min-max"
      |and removeOutlierValue="false"
      |;
      |
      |load parquet.`/tmp/scaler/data`
      |as featurize_table;
      |;
    """.stripMargin)

  final val inputCols: Param[String] = new Param[String](this, "inputCols", FormParams.toJson(Text(
    name = "inputCols",
    value = "",
    extra = Extra(
      doc =
        """
          |Which text columns you want to process.
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "string",
        "required" -> "true",
        "derivedType" -> "NONE"
      )))
  ))

  final val scaleMethod: Param[String] = new Param[String](this, "scaleMethod", FormParams.toJson(Select(
    name = "scaleMethod",
    values = List(),
    extra = Extra(
      doc =
        """
          |What kind of scale method you want to process.
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "string",
        "required" -> "false",
        "derivedType" -> "NONE"
      )),valueProvider= Option(()=>{
      List(
        KV(Option("scaledMethod"), Option("log2")),
        KV(Option("scaledMethod"), Option("min-max")),
        KV(Option("scaledMethod"), Option("logn")),
        KV(Option("scaledMethod"), Option("log10")),
        KV(Option("scaledMethod"), Option("sqrt")),
        KV(Option("scaledMethod"), Option("abs"))
      )
    }))
  ))
  setDefault(scaleMethod, "log2")

  final val removeOutlierValue: Param[String] = new Param[String](this, "removeOutlierValue", FormParams.toJson(Select(
    name = "removeOutlierValue",
    values= List(),
    extra = Extra(
      doc =
        """
          |Whether to remove outlier values.
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "string",
        "required" -> "false",
        "derivedType" -> "NONE"
      )), valueProvider = Option(()=>{
      List(
        KV(Option("removeOutlierValue"),Option("true")),
        KV(Option("removeOutlierValue"),Option("false"))
      )
    }))
  ))
  setDefault(removeOutlierValue, "false")

}
