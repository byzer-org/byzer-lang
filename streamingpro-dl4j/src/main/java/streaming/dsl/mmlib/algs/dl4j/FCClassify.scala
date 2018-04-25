package streaming.dsl.mmlib.algs.dl4j

import java.util.Random

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{DenseLayer, OutputLayer}
import org.deeplearning4j.nn.conf.{NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.weights.WeightInit
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.lossfunctions.LossFunctions
import streaming.dl4j.Dl4jFunctions
import streaming.dsl.mmlib.SQLAlg


/**
  * Created by allwefantasy on 23/2/2018.
  */
class FCClassify extends SQLAlg with Dl4jFunctions {
  def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    dl4jClassificationTrain(df, path, params, () => {

      val featureSize = params.getOrElse("featureSize", "-1").toInt
      val labelSize = params.getOrElse("labelSize", "-1").toInt
      val learningRate = params.getOrElse("learningRate", "0.001").toDouble
      val layerGroup = params.getOrElse("layerGroup", "300,100")

      val layers = new NeuralNetConfiguration.Builder()
        .seed(new Random(System.currentTimeMillis()).nextInt(9999999))
        .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
        .iterations(1)
        .activation(Activation.LEAKYRELU)
        .weightInit(WeightInit.XAVIER)
        .learningRate(learningRate)
        .updater(Updater.NESTEROVS) // To configure: .updater(Nesterovs.builder().momentum(0.9).build())
        .regularization(true).l2(1e-4)
        .list()

      var nIn = featureSize
      var finalLayers = layers
      var layer_count = 0
      layerGroup.split(",").map(f => f.toInt).foreach { size =>
        finalLayers = layers.layer(layer_count, new DenseLayer.Builder().nIn(nIn).nOut(size).activation(Activation.RELU).build())
        nIn = size
        layer_count += 1
      }

      finalLayers = finalLayers.layer(layer_count, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
        .activation(Activation.SOFTMAX).nIn(nIn).nOut(labelSize).build())

      val netConf = finalLayers.pretrain(false).backprop(true)
        .build()

      netConf

    })
  }

  override def load(sparkSession: SparkSession, path: String): Any = {
    null
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
     null
  }
}
