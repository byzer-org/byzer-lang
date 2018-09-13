package streaming.dsl.mmlib.algs.dl4j

import java.util.Random

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.variational.{BernoulliReconstructionDistribution, VariationalAutoencoder}
import org.deeplearning4j.nn.conf.{NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.weights.WeightInit
import org.nd4j.linalg.activations.Activation
import streaming.dl4j.Dl4jFunctions
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 24/2/2018.
  */
class SDAutoencoder extends SQLAlg with Dl4jFunctions {

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    dl4jClassificationTrain(df, path, params, () => {

      val featureSize = params.getOrElse("featureSize", "-1").toInt
      val labelSize = params.getOrElse("labelSize", "-1").toInt
      val learningRate = params.getOrElse("learningRate", "0.001").toDouble
      val layerGroup = params.getOrElse("layerGroup", "300,100")

      val layers = new NeuralNetConfiguration.Builder()
        .seed(new Random(System.currentTimeMillis()).nextInt(9999999))
        .learningRate(learningRate)
        .updater(Updater.RMSPROP)
        .weightInit(WeightInit.XAVIER)
        //.updater(Nesterovs.builder().momentum(0.5).momentumSchedule(Collections.singletonMap(3, 0.9)).learningRate(learningRate).build())
        .optimizationAlgo(OptimizationAlgorithm.CONJUGATE_GRADIENT)
        .regularization(true).l2(1e-4)
        .list()

      var finalLayers = layers

      val encoderGroup = layerGroup.split(",").map(f => f.toInt)

      finalLayers = finalLayers.layer(0, new VariationalAutoencoder.Builder()
        .activation(Activation.LEAKYRELU)
        .encoderLayerSizes(encoderGroup: _*) //2 encoder layers, each of size 256
        .decoderLayerSizes(encoderGroup: _*) //2 decoder layers, each of size 256
        .pzxActivationFunction(Activation.IDENTITY) //p(z|data) activation function
        .reconstructionDistribution(new BernoulliReconstructionDistribution(Activation.RELU.getActivationFunction())) //Bernoulli distribution for p(data|z) (binary or 0 to 1 data only)
        .nIn(featureSize) //Input size: 28x28
        .nOut(labelSize) //Size of the latent variable space: p(z|x). 2 dimensions here for plotting, use more in general
        .build())

      val netConf = finalLayers
        .pretrain(true)
        .backprop(false)
        .build()

      netConf

    })
    import df.sparkSession.implicits._
    Seq.empty[String].toDF("name")
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = null

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }
}
