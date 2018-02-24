package streaming.dsl.mmlib.algs.dl4j

import streaming.dl4j.DL4JModelLoader
import streaming.dl4j.DL4JModelPredictor
import java.io.File
import java.util.{Collections, Random}

import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.nn.api.{Model, OptimizationAlgorithm}
import org.deeplearning4j.nn.conf.layers.{DenseLayer, OutputLayer}
import org.deeplearning4j.nn.conf.{NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.api.IterationListener
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.deeplearning4j.spark.api.RDDTrainingApproach
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.deeplearning4j.spark.parameterserver.training.SharedTrainingMaster
import org.deeplearning4j.util.ModelSerializer
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions
import org.nd4j.parameterserver.distributed.conf.VoidConfiguration


/**
  * Created by allwefantasy on 23/2/2018.
  */
class FCClassify extends SQLAlg with Functions {

  def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    require(params.contains("featureSize"), "featureSize is required")
    require(params.contains("labelSize"), "labelSize is required")

    val featureSize = params.getOrElse("featureSize", "-1").toInt
    val labelSize = params.getOrElse("labelSize", "-1").toInt
    val batchSize = params.getOrElse("batchSize", "32").toInt
    val unicastPort = params.getOrElse("unicastPort", (new Random(System.currentTimeMillis()).nextInt(65535 - 49152) + 49152).toString).toInt
    val updatesThreshold = params.getOrElse("updatesThreshold", "0.003").toDouble
    val workersPerNode = params.getOrElse("workersPerNode", "1").toInt
    val learningRate = params.getOrElse("learningRate", "0.001").toDouble
    val layerGroup = params.getOrElse("layerGroup", "300,100")
    val epochs = params.getOrElse("epochs", "1").toInt
    val validateTable = params.getOrElse("validateTable", "")

    val voidConfiguration = VoidConfiguration.builder()
      .unicastPort(unicastPort)
      .controllerAddress(if (df.sparkSession.sparkContext.isLocal) "127.0.0.1" else null)
      .build()

    val tm = new SharedTrainingMaster.Builder(voidConfiguration, batchSize)
      .updatesThreshold(updatesThreshold)
      .rddTrainingApproach(RDDTrainingApproach.Export)
      .batchSizePerWorker(batchSize)
      .workersPerNode(workersPerNode)
      .build()

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

    val sparkNetwork = new SparkDl4jMultiLayer(df.sparkSession.sparkContext, netConf, tm)
    sparkNetwork.setListeners(Collections.singletonList[IterationListener](new ScoreIterationListener(1)));

    val newDataSetRDD = df.select(params.getOrElse("inputCol", "features"), params.getOrElse("outputCol", "label")).rdd.map { row =>
      val features = row.getAs[Vector](0)
      val label = row.getAs[Vector](1)
      new org.nd4j.linalg.dataset.DataSet(Nd4j.create(features.toArray), Nd4j.create(label.toArray))
    }.toJavaRDD()

    (0 until epochs).foreach { i =>
      sparkNetwork.fit(newDataSetRDD)
    }

    val tempModelLocalPath = createTempModelLocalPath(path)
    ModelSerializer.writeModel(sparkNetwork.getNetwork, new File(tempModelLocalPath, "dl4j.model"), true)
    copyToHDFS(tempModelLocalPath + "/dl4j.model", path + "/dl4j.model", true)

    if (!validateTable.isEmpty) {

      val testDataSetRDD = df.sparkSession.table(validateTable).select(params.getOrElse("inputCol", "features"), params.getOrElse("outputCol", "label")).rdd.map { row =>
        val features = row.getAs[Vector](0)
        val label = row.getAs[Vector](1)
        new org.nd4j.linalg.dataset.DataSet(Nd4j.create(features.toArray), Nd4j.create(label.toArray))
      }.toJavaRDD()

      val evaluation = sparkNetwork.doEvaluation(testDataSetRDD, batchSize, new Evaluation(labelSize))(0); //Work-around for 0.9.1 bug: see https://deeplearning4j.org/releasenotes
      logger.info("***** Evaluation *****")
      logger.info(evaluation.stats())
      logger.info("***** Example Complete *****")
    }
    tm.deleteTempFiles(df.sparkSession.sparkContext)
  }

  override def load(sparkSession: SparkSession, path: String): Any = {
    null
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
    null
  }
}
