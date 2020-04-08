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

import net.csdn.common.reflect.ReflectHelper
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, MLSQLUtils, SparkSession}
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams

import scala.collection.mutable.ArrayBuffer


/**
  * Created by allwefantasy on 12/9/2018.
  */
class SQLXGBoostExt(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseClassification {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {


    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
    setKeepVersion(keepVersion)

    val evaluateTable = params.get("evaluateTable")
    setEvaluateTable(evaluateTable.getOrElse("None"))

    SQLPythonFunc.incrementVersion(path, keepVersion)
    val spark = df.sparkSession

    trainModelsWithMultiParamGroup2(df, path, params, () => {
      val obj = Class.forName("streaming.dsl.mmlib.algs.XGBoostExt").newInstance()
      ReflectHelper.method(obj, "WowXGBoostClassifier").asInstanceOf[Params]
    }, (_model, fitParam) => {
      evaluateTable match {
        case Some(etable) =>
          val model = _model.asInstanceOf[Transformer]
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

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val (bestModelPath, baseModelPath, metaPath) = mllibModelAndMetaPath(path, params, sparkSession)
    val obj = Class.forName("streaming.dsl.mmlib.algs.XGBoostExt").newInstance()
    val model = ReflectHelper.method(obj, "load", bestModelPath(0))
    ArrayBuffer(model)
  }


  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession, () => {
      val obj = Class.forName("streaming.dsl.mmlib.algs.XGBoostExt").newInstance()
      ReflectHelper.method(obj, "WowXGBoostClassifier").asInstanceOf[Params]
    })
  }


  override def explainModel(sparkSession: SparkSession, path: String, params: Map[String, String]): DataFrame = {
    val obj = Class.forName("streaming.dsl.mmlib.algs.XGBoostExt").newInstance()
    ReflectHelper.method(obj, "explainModel", sparkSession, load(sparkSession, path, params).asInstanceOf[ArrayBuffer[_]]).asInstanceOf[DataFrame]
  }

  override def doc: Doc = Doc(MarkDownDoc,
    """
      |XGBoostExt is based on [xgboost4j-spark](https://xgboost.readthedocs.io/en/latest/jvm/scaladocs/xgboost4j-spark/index.html).
      |
      |If you wanna use this module, compile StreamingPro with -Pstreamingpro-xgboost enabled.
      |
      |Check model params:
      |
      | ```sql
      | load modelExplain.`/tmp/model` where alg="XGBoostExt" as outout;
      | ```
      |
      |Check Alg params:
      |
      |```sql
      | load modelParam.`XGBoostExt`  as outout;
      |```
      |
     """.stripMargin)

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |load jsonStr.`jsonStr` as data;
      |select vec_dense(features) as features ,label as label from data
      |as data1;
      |
      |-- batch predict
      |train data as XGBoostExt.`/tmp/model`;
      |predict data as XGBoostExt.`/tmp/model`;
      |
      |-- api predict
      |register XGBoostExt.`/tmp/model` as npredict;
      |select npredict(features) from data as output;
      |
    """.stripMargin)

  override def modelType: ModelType = AlgType

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    val models = load(spark, path, params).asInstanceOf[ArrayBuffer[Transformer]]
    models.head.transform(df)
  }

  override def predict(sparkSession: _root_.org.apache.spark.sql.SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val models = sparkSession.sparkContext.broadcast(_model.asInstanceOf[ArrayBuffer[Any]])

    val f = (vec: Vector) => {
      models.value.map { model =>
        model.getClass.getMethod("predict", classOf[Vector]).invoke(model, vec).asInstanceOf[Double]
      }.sortBy(f => f).reverse.head
    }

    MLSQLUtils.createUserDefinedFunction(f, DoubleType, Some(Seq(VectorType)))
  }

  override def coreCompatibility: Seq[CoreVersion] = Seq(Core_2_3_x)
}
