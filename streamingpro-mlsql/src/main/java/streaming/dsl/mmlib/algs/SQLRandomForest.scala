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

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import streaming.dsl.mmlib._

import scala.collection.mutable.ArrayBuffer
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLRandomForest(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseClassification {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {


    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
    setKeepVersion(keepVersion)

    val evaluateTable = params.get("evaluateTable")
    setEvaluateTable(evaluateTable.getOrElse("None"))

    SQLPythonFunc.incrementVersion(path, keepVersion)
    val spark = df.sparkSession

    trainModelsWithMultiParamGroup[RandomForestClassificationModel](df, path, params, () => {
      new RandomForestClassifier()
    }, (_model, fitParam) => {
      evaluateTable match {
        case Some(etable) =>
          val model = _model.asInstanceOf[RandomForestClassificationModel]
          val evaluateTableDF = spark.table(etable)
          val predictions = model.transform(evaluateTableDF)
          multiclassClassificationEvaluate(predictions, (evaluator) => {
            evaluator.setLabelCol(fitParam.getOrElse("labelCol", "label"))
            evaluator.setPredictionCol("prediction")
          })

        case None => List()
      }
    }
    )

    formatOutput(getModelMetaData(spark, path))
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val model = load(df.sparkSession, path, params).asInstanceOf[ArrayBuffer[RandomForestClassificationModel]].head
    model.transform(df)
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession, () => {
      new RandomForestClassifier()
    })
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {

    val (bestModelPath, baseModelPath, metaPath) = mllibModelAndMetaPath(path, params, sparkSession)
    val model = RandomForestClassificationModel.load(bestModelPath(0))
    ArrayBuffer(model)
  }


  override def explainModel(sparkSession: SparkSession, path: String, params: Map[String, String]): DataFrame = {
    val models = load(sparkSession, path, params).asInstanceOf[ArrayBuffer[RandomForestClassificationModel]]
    val rows = models.flatMap { model =>
      val modelParams = model.params.filter(param => model.isSet(param)).map { param =>
        val tmp = model.get(param).get
        val str = if (tmp == null) {
          "null"
        } else tmp.toString
        Seq(("fitParam.[group]." + param.name), str)
      }
      Seq(
        Seq("uid", model.uid),
        Seq("numFeatures", model.numFeatures.toString),
        Seq("numClasses", model.numClasses.toString),
        Seq("numTrees", model.treeWeights.length.toString),
        Seq("treeWeights", model.treeWeights.mkString(","))
      ) ++ modelParams
    }.map(Row.fromSeq(_))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows, 1),
      StructType(Seq(StructField("name", StringType), StructField("value", StringType))))
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    predict_classification(sparkSession, _model, name)
  }

  override def modelType: ModelType = AlgType

  override def doc: Doc = Doc(HtmlDoc,
    """
      | <a href="http://en.wikipedia.org/wiki/Random_forest">Random Forest</a> learning algorithm for
      | classification.
      | It supports both binary and multiclass labels, as well as both continuous and categorical
      | features.
      |
      | Use "load modelParams.`RandomForest` as output;"
      |
      | to check the available hyper parameters;
      |
      | Use "load modelExample.`RandomForest` as output;"
      | get example.
      |
      | If you wanna check the params of model you have trained, use this command:
      |
      | ```
      | load modelExplain.`/tmp/model` where alg="RandomForest" as outout;
      | ```
      |
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |load jsonStr.`jsonStr` as data;
      |select vec_dense(features) as features ,label as label from data
      |as data1;
      |
      |-- use RandomForest
      |train data1 as RandomForest.`/tmp/model` where
      |
      |-- once set true,every time you run this script, MLSQL will generate new directory for you model
      |keepVersion="true"
      |
      |-- specify the test dataset which will be used to feed evaluator to generate some metrics e.g. F1, Accurate
      |and evaluateTable="data1"
      |
      |-- specify group 0 parameters
      |and `fitParam.0.labelCol`="features"
      |and `fitParam.0.featuresCol`="label"
      |and `fitParam.0.maxDepth`="2"
      |
      |-- specify group 1 parameters
      |and `fitParam.1.featuresCol`="features"
      |and `fitParam.1.labelCol`="label"
      |and `fitParam.1.maxDepth`="10"
      |;
    """.stripMargin)

}
