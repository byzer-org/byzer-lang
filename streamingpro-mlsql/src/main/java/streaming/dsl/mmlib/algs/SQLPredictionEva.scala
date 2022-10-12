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
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib._
import tech.mlsql.common.form.{Extra, FormParams, Text}

/**
 * Created by allwefantasy on 24/7/2018.
 */
class SQLPredictionEva(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams {

  def this() = this(BaseParams.randomUID())

  private def binaryClassificationEvaluation(df: DataFrame, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._
    val labelCol = params.getOrElse("labelCol", "label")
    val labelVal = params.getOrElse("labelValue", 1.0)
    val probCol = params.getOrElse("probCol", "prob")
    val predictionDf = df.select(probCol, labelCol)
    val predictionRdd = predictionDf.as[(Double, Double)].rdd
    val binaryClassMetrics = new BinaryClassificationMetrics(predictionRdd)
    //    val accuracy = binaryClassMe
    val threshold = params.getOrElse("threshold", 0.5).asInstanceOf[Double]
    val TP = predictionDf.filter(s"$probCol>=$threshold and $labelCol==$labelVal").count().toDouble
    val FP = predictionDf.filter(s"$probCol>=$threshold and $labelCol!=$labelVal").count().toDouble
    val TN = predictionDf.filter(s"$probCol<$threshold and $labelCol!=$labelVal").count().toDouble
    val FN = predictionDf.filter(s"$probCol<$threshold and $labelCol==$labelVal").count().toDouble
    val precision = TP / (TP + FP)
    val recall = TP / (TP + FN)
    val f1 = 2 * TP / (2 * TP + FP + FN)
    val auc = binaryClassMetrics.areaUnderROC()
    val prc = binaryClassMetrics.areaUnderPR()
    val rows = Seq(
      Seq("threshold", threshold.toString),
      Seq("precision", precision.toString),
      Seq("recall", recall.toString),
      Seq("f1", f1.toString),
      Seq("auc", auc.toString),
      Seq("prc", prc.toString)
    ).map(Row.fromSeq(_))
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 1),
      StructType(Seq(StructField("metrics", StringType), StructField("value", StringType)))
    )
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    binaryClassificationEvaluation(df, params)
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }

  final val labelCol: Param[String] = new Param[String](this, "labelCol", FormParams.toJson(Text(
    name = "labelCol",
    value = "",
    extra = Extra(
      doc =
        """
          |Specify the column name that contains the prediction result
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "string",
        "defaultValue" -> "label",
        "required" -> "true",
        "derivedType" -> "NONE"
      )))
  ))

  final val labelVal: Param[String] = new Param[String](this, "labelVal", FormParams.toJson(Text(
    name = "labelVal",
    value = "",
    extra = Extra(
      doc =
        """
          |Specify the value reveals the positive samples.
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "double",
        "defaultValue" -> "1.0",
        "required" -> "false",
        "derivedType" -> "NONE"
      )))
  ))

  final val probCol: Param[String] = new Param[String](this, "probCol", FormParams.toJson(Text(
    name = "probCol",
    value = "",
    extra = Extra(
      doc =
        """
          |Specify the column of the probability value.
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "string",
        "defaultValue" -> "prob",
        "required" -> "true",
        "derivedType" -> "NONE"
      )))
  ))

  final val threshold: Param[String] = new Param[String](this, "threshold", FormParams.toJson(Text(
    name = "threshold",
    value = "0.5",
    extra = Extra(
      doc =
        """
          | The value that defines whether a sample record is positive or not.
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "double",
        "defaultValue" -> "0.5",
        "required" -> "false",
        "derivedType" -> "NONE"
      )))
  ))

  override def load(sparkSession: SparkSession, _path: String, params: Map[String, String]): Any = {
    throw new RuntimeException("register is not supported in PredictionEva")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException("register is not supported in PredictionEva")
  }

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |select replace(String(probability), "[","") as prob, label from res as res1;
      |select replace(prob, "]", "") as prob, label from res1 as res2;
      |select Double(split(prob, ",")[1]) as prob, label from res2 as res3;
      |run res3 as PredictionEva.`` where labelCol='label' and probCol='prob';
      |;
    """.stripMargin)

  override def doc: Doc = Doc(HtmlDoc,
    """
      | <a href="https://en.wikipedia.org/wiki/Evaluation_of_binary_classifiers"> PredictionEva </a> is an extension for evaluating the prediction result.
      |
      | It will evaluate the prediction results from f1-score, precision, recall, auc value and prc value.
      |
    """.stripMargin)
}