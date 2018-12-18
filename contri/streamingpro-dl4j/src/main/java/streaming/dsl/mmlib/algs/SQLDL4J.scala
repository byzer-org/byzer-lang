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

import java.util.Random

import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.spark.api.RDDTrainingApproach
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster
import org.deeplearning4j.spark.parameterserver.training.SharedTrainingMaster
import org.nd4j.parameterserver.distributed.conf.VoidConfiguration
import streaming.dl4j.{DL4JModelLoader, DL4JModelPredictor, Dl4jFunctions}
import streaming.dsl.mmlib.SQLAlg


/**
  * Created by allwefantasy on 15/1/2018.
  */
class SQLDL4J extends SQLAlg with Dl4jFunctions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    require(params.contains("featureSize"), "featureSize is required")
    require(params.contains("labelSize"), "labelSize is required")
    require(params.contains("alg"), "alg is required")
    /*
     NTP error please configure:
       --conf spark.driver.extraJavaOptions=-Dorg.deeplearning4j.spark.time.TimeSource=org.deeplearning4j.spark.time.SystemClockTimeSource
       --conf spark.executor.extraJavaOptions=-Dorg.deeplearning4j.spark.time.TimeSource=org.deeplearning4j.spark.time.SystemClockTimeSource

     JVM crash please configure:
       --conf "spark.executor.extraJavaOptions=-Dorg.bytedeco.javacpp.maxbytes=5368709120"
       --conf "spark.driver.extraJavaOptions=-Dorg.bytedeco.javacpp.maxbytes=5368709120"
       --conf spark.yarn.executor.memoryOverhead=6144
     */
    Class.forName("streaming.dsl.mmlib.algs.dl4j." + params("alg")).newInstance().asInstanceOf[SQLAlg].train(df, path, params)
    import df.sparkSession.implicits._
    Seq.empty[String].toDF("name")
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    path
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val f = (v: org.apache.spark.ml.linalg.Vector, runType: String) => {
      val modelBundle = DL4JModelLoader.load(_model.asInstanceOf[String])
      val res = runType match {
        case "classification" =>
          DL4JModelPredictor.run_double(modelBundle.asInstanceOf[MultiLayerNetwork], v)
        case "encoder" => DL4JModelPredictor.activate(modelBundle.asInstanceOf[MultiLayerNetwork], v)
        case _ => DL4JModelPredictor.run_double(modelBundle.asInstanceOf[MultiLayerNetwork], v)
      }
      res
    }
    UserDefinedFunction(f, VectorType, Some(Seq(VectorType, StringType)))
  }
}

object SQLDL4J {

  def init(isLocal: Boolean, updatesThreshold: Double = 0.003, batchSizePerWorker: Int = 32, workersPerNode: Int = 1) = {
    val unicastPort = new Random(System.currentTimeMillis()).nextInt(65535 - 49152) + 49152

    val voidConfiguration = VoidConfiguration.builder()
      .unicastPort(unicastPort)
      .controllerAddress(if (isLocal) "127.0.0.1" else null)
      .build()

    val tm = new SharedTrainingMaster.Builder(voidConfiguration, batchSizePerWorker)
      .updatesThreshold(updatesThreshold)
      .rddTrainingApproach(RDDTrainingApproach.Export)
      .batchSizePerWorker(batchSizePerWorker)
      .workersPerNode(workersPerNode)
      .build()
    tm
  }

  def init2(isLocal: Boolean, averagingFrequency: Int = 5, batchSizePerWorker: Int = 32, workerPrefetchNumBatches: Int = 2) = {
    val tm = new ParameterAveragingTrainingMaster.Builder(batchSizePerWorker)
      .averagingFrequency(averagingFrequency)
      .workerPrefetchNumBatches(workerPrefetchNumBatches)
      .batchSizePerWorker(batchSizePerWorker)
      .build()
    tm
  }

}
