package streaming.dl4j

import streaming.common.HDFSOperator
import java.io.File
import java.util.Collections

import net.csdn.common.logging.Loggers
import streaming.dsl.mmlib.algs.SQLDL4J
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql._
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.nn.conf.MultiLayerConfiguration
import org.deeplearning4j.optimize.api.IterationListener
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.deeplearning4j.util.ModelSerializer
import org.nd4j.linalg.factory.Nd4j


/**
  * Created by allwefantasy on 25/4/2018.
  */
trait Dl4jFunctions {
  val logger = Loggers.getLogger(getClass)

  def dl4jClassificationTrain(df: DataFrame, path: String, params: Map[String, String], multiLayerConfiguration: () => MultiLayerConfiguration): Unit = {
    require(params.contains("featureSize"), "featureSize is required")

    val labelSize = params.getOrElse("labelSize", "-1").toInt
    val batchSize = params.getOrElse("batchSize", "32").toInt

    val epochs = params.getOrElse("epochs", "1").toInt
    val validateTable = params.getOrElse("validateTable", "")

    val tm = SQLDL4J.init2(df.sparkSession.sparkContext.isLocal, batchSizePerWorker = batchSize)

    val netConf = multiLayerConfiguration()

    val sparkNetwork = new SparkDl4jMultiLayer(df.sparkSession.sparkContext, netConf, tm)
    sparkNetwork.setCollectTrainingStats(false)
    sparkNetwork.setListeners(Collections.singletonList[IterationListener](new ScoreIterationListener(1)))

    val labelFieldName = params.getOrElse("outputCol", "label")
    val newDataSetRDD = if (df.schema.fieldNames.contains(labelFieldName)) {

      require(params.contains("labelSize"), "labelSize is required")

      df.select(params.getOrElse("inputCol", "features"), params.getOrElse("outputCol", "label")).rdd.map { row =>
        val features = row.getAs[Vector](0)
        val label = row.getAs[Vector](1)
        new org.nd4j.linalg.dataset.DataSet(Nd4j.create(features.toArray), Nd4j.create(label.toArray))
      }.toJavaRDD()
    } else {
      df.select(params.getOrElse("inputCol", "features")).rdd.map { row =>
        val features = row.getAs[Vector](0)
        new org.nd4j.linalg.dataset.DataSet(Nd4j.create(features.toArray), Nd4j.zeros(0))
      }.toJavaRDD()
    }


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

  def copyToHDFS(tempModelLocalPath: String, path: String, clean: Boolean) = {
    HDFSOperator.copyToHDFS(tempModelLocalPath, path, clean)
  }

  def createTempModelLocalPath(path: String, autoCreateParentDir: Boolean = true) = {
    HDFSOperator.createTempModelLocalPath(path, autoCreateParentDir)
  }
}
