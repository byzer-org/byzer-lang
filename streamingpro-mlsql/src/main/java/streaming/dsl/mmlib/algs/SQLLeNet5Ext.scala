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

import com.intel.analytics.bigdl.dlframes.{DLClassifier, DLModel}
import com.intel.analytics.bigdl.models.lenet.LeNet5
import com.intel.analytics.bigdl.models.utils.ModelBroadcast
import com.intel.analytics.bigdl.nn.{ClassNLLCriterion, Module}
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.utils.Engine
import net.sf.json.JSONArray
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.bigdl.BigDLFunctions
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.utils.hdfs.HDFSOperator

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class SQLLeNet5Ext(override val uid: String) extends SQLAlg with MllibFunctions with BigDLFunctions with BaseClassification {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    Engine.init
    params.get(keepVersion.name).
      map(m => set(keepVersion, m.toBoolean)).
      getOrElse($(keepVersion))

    val eTable = params.get(evaluateTable.name)

    SQLPythonFunc.incrementVersion(path, $(keepVersion))
    val spark = df.sparkSession


    bigDLClassifyTrain[Float](df, path, params, (newFitParam) => {
      val model = LeNet5(classNum = newFitParam("classNum").toInt)
      val criterion = ClassNLLCriterion[Float]()
      val alg = new DLClassifier[Float](model, criterion,
        JSONArray.fromObject(newFitParam("featureSize")).map(f => f.asInstanceOf[Int]).toArray)
      alg
    }, (_model, newFitParam) => {
      eTable match {
        case Some(etable) =>
          val model = _model
          val evaluateTableDF = spark.table(etable)
          val predictions = model.transform(evaluateTableDF)
          multiclassClassificationEvaluate(predictions, (evaluator) => {
            evaluator.setLabelCol(newFitParam.getOrElse("labelCol", "label"))
            evaluator.setPredictionCol("prediction")
          })

        case None => List()
      }
    })

    formatOutput(getModelMetaData(spark, path))
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    Engine.init
    val models = load(df.sparkSession, path, params).asInstanceOf[ArrayBuffer[DLModel[Float]]]
    models.head.transform(df)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    Engine.init
    val (bestModelPath, baseModelPath, metaPath) = mllibModelAndMetaPath(path, params, sparkSession)
    val trainParams = sparkSession.read.parquet(metaPath + "/0").collect().head.getAs[Map[String, String]]("trainParams")

    val featureSize = JSONArray.fromObject(trainParams("featureSize")).map(f => f.asInstanceOf[Int]).toArray

    val model = Module.loadModule[Float](HDFSOperator.getFilePath(bestModelPath(0)))
    val dlModel = new DLModel[Float](model, featureSize)
    ArrayBuffer(dlModel)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val dlmodel = _model.asInstanceOf[ArrayBuffer[DLModel[Float]]].head
    val featureSize = dlmodel.featureSize
    val modelBroadCast = ModelBroadcast[Float]().broadcast(sparkSession.sparkContext, dlmodel.model.evaluate())


    val f = (vec: Vector) => {

      val localModel = modelBroadCast.value()
      val featureTensor = Tensor(vec.toArray.map(f => f.toFloat), featureSize)
      val output = localModel.forward(featureTensor)
      val res = output.toTensor[Float].clone().storage().array().map(f => f.toDouble)
      Vectors.dense(res)
    }

    UserDefinedFunction(f, VectorType, Some(Seq(VectorType)))
  }


  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession, () => {
      val model = LeNet5(classNum = 10)
      val criterion = ClassNLLCriterion[Float]()
      val alg = new DLClassifier[Float](model, criterion, Array(28, 28))
      alg
    })
  }

  override def modelType: ModelType = AlgType

  override def doc: Doc = Doc(MarkDownDoc,
    """
      |
      |load modelParams.`LeNet5Ext`  as output;
    """.stripMargin)

  override def codeExample: Code = Code(SQLCode,
    """
      |set json = '''{}''';
      |load jsonStr.`json` as emptyData;
      |
      |run emptyData as MnistLoaderExt.`` where
      |mnistDir="/Users/allwefantasy/Downloads/mnist"
      |as data;
      |
      |train data as LeNet5Ext.`/tmp/lenet` where
      |fitParam.0.featureSize="[28,28]"
      |and fitParam.0.classNum="10";
      |
      |predict data as LeNet5Ext.`/tmp/lenet`;
      |
      |register LeNet5Ext.`/tmp/lenet` as mnistPredict;
      |
      |select
      |vec_argmax(mnistPredict(vec_dense(features))) as predict_label,
      |label from data
      |as output;
    """.stripMargin)
}

case class BigDLDefaultConfig(batchSize: Int = 128, maxEpoch: Int = 1)
