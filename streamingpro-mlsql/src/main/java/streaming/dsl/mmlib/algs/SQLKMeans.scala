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

import org.apache.spark.ml.clustering.{BisectingKMeans, BisectingKMeansModel}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, MLSQLUtils, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.IntegerType
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.cluster.BaseCluster
import streaming.dsl.mmlib.algs.param.BaseParams

/**
  * Created by allwefantasy on 14/1/2018.
  */
class SQLKMeans(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseCluster {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
    setKeepVersion(keepVersion)

    val evaluateTable = params.get("evaluateTable")
    setEvaluateTable(evaluateTable.getOrElse("None"))


    SQLPythonFunc.incrementVersion(path, keepVersion)
    val spark = df.sparkSession

    trainModelsWithMultiParamGroup[BisectingKMeansModel](df, path, params, () => {
      new BisectingKMeans()
    }, (_model, fitParam) => {
      evaluateTable match {
        case Some(etable) =>
          val model = _model.asInstanceOf[BisectingKMeansModel]
          val evaluateTableDF = spark.table(etable)
          val predictions = model.transform(evaluateTableDF)
          clusterEvaluate(predictions, (evaluator) => {
            evaluator.setFeaturesCol(params.getOrElse("featuresCol", "features"))
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
    val model = BisectingKMeansModel.load(bestModelPath(0))
    model
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession, () => {
      new BisectingKMeans()
    })
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[BisectingKMeansModel])
    val f = (v: Vector) => {
      model.value.getClass.getDeclaredMethod("predict", classOf[Vector]).invoke(model.value, v).asInstanceOf[Int]
    }
    MLSQLUtils.createUserDefinedFunction(f, IntegerType, Some(Seq(VectorType)))
  }
}
