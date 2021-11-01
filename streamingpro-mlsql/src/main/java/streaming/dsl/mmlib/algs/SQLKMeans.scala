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
import streaming.dsl.mmlib.{Code, Doc, HtmlDoc, SQLAlg, SQLCode}
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

  override def doc: Doc = Doc(HtmlDoc,
    """
      |<a href="https://en.wikipedia.org/wiki/K-means_clustering"> k-means clustering </a>
      |
      |k-means clustering is a method of vector quantization, originally from signal processing,
      |that aims to partition n observations into k clusters in which each observation belongs to
      |the cluster with the nearest mean (cluster centers or cluster centroid), serving as a prototype
      |of the cluster. This results in a partitioning of the data space into Voronoi cells.
      |k-means clustering minimizes within-cluster variances (squared Euclidean distances),
      |but not regular Euclidean distances, which would be the more difficult Weber problem:
      |the mean optimizes squared errors, whereas only the geometric median minimizes Euclidean distances.
      |For instance, better Euclidean solutions can be found using k-medians and k-medoids.
      |
      | Use "load modelParams.`KMeans` as output;"
      |
      | to check the available hyper parameters;
      |
      |""".stripMargin
  )

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |
      |set jsonStr='''
      |{"features":[5.1,3.5,1.4,0.2],"label":0.0},
      |{"features":[5.1,3.5,1.4,0.2],"label":1.0}
      |{"features":[5.1,3.5,1.4,0.2],"label":0.0}
      |{"features":[4.4,2.9,1.4,0.2],"label":0.0}
      |{"features":[5.1,3.5,1.4,0.2],"label":1.0}
      |{"features":[5.1,3.5,1.4,0.2],"label":0.0}
      |{"features":[5.1,3.5,1.4,0.2],"label":0.0}
      |{"features":[4.7,3.2,1.3,0.2],"label":1.0}
      |{"features":[5.1,3.5,1.4,0.2],"label":0.0}
      |{"features":[5.1,3.5,1.4,0.2],"label":0.0}
      |''';
      |load jsonStr.`jsonStr` as data;
      |select vec_dense(features) as features from data
      |as data1;
      |train data1 as KMeans.`/tmp/alg/kmeans`
      |where k="2"
      |and seed="1";
      |
      |
      |""".stripMargin
  )


  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[BisectingKMeansModel])
    val f = (v: Vector) => {
      model.value.getClass.getDeclaredMethod("predict", classOf[Vector]).invoke(model.value, v).asInstanceOf[Int]
    }
    MLSQLUtils.createUserDefinedFunction(f, IntegerType, Some(Seq(VectorType)))
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val model = load(df.sparkSession, path, params).asInstanceOf[BisectingKMeansModel]
    model.transform(df)
  }
}
