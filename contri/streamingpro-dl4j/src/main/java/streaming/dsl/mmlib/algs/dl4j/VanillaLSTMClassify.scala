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

package streaming.dsl.mmlib.algs.dl4j

import java.util.Random

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{GravesLSTM, OutputLayer}
import org.deeplearning4j.nn.conf.{NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.weights.WeightInit
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.lossfunctions.LossFunctions
import streaming.dl4j.Dl4jFunctions
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 26/2/2018.
  */
class VanillaLSTMClassify extends SQLAlg with Dl4jFunctions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
      dl4jClassificationTrain(df, path, params, () => {

        val featureSize = params.getOrElse("featureSize", "-1").toInt
        val wordEmbeddingSize = params.getOrElse("wordEmbeddingSize", "-1").toInt
        val labelSize = params.getOrElse("labelSize", "-1").toInt
        val learningRate = params.getOrElse("learningRate", "0.001").toDouble
        val layerGroup = params.getOrElse("layerGroup", "300")

        val layers = new NeuralNetConfiguration.Builder()
          .seed(new Random(System.currentTimeMillis()).nextInt(9999999))
          .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
          .iterations(1)
          .regularization(true)
          .activation(Activation.LEAKYRELU)
          .weightInit(WeightInit.XAVIER)
          .learningRate(learningRate)
          .updater(Updater.ADAM)
          .regularization(true).l2(1e-4)
          .list()

        var nIn = featureSize / wordEmbeddingSize
        var finalLayers = layers
        var layer_count = 0
        layerGroup.split(",").map(f => f.toInt).foreach { size =>
          finalLayers = layers.layer(layer_count, new GravesLSTM.Builder().nIn(nIn).nOut(size).activation(Activation.RELU).build())
          nIn = size
          layer_count += 1
        }

        finalLayers = finalLayers.layer(layer_count, new OutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
          .activation(Activation.SOFTMAX).nIn(nIn).nOut(labelSize).build())

        val netConf = finalLayers.pretrain(false).backprop(true)
          .build()

        netConf

      })
    }
    import df.sparkSession.implicits._
    Seq.empty[String].toDF("name")

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = null

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = null
}
