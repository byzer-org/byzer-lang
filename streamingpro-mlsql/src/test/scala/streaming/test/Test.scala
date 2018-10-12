package streaming.test

import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}

import org.apache.http.client.fluent.{Form, Request}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.dataset.image.{BytesToGreyImg, GreyImgNormalizer, GreyImgToBatch}
import com.intel.analytics.bigdl.dataset.{DataSet, DistributedDataSet, MiniBatch, _}
import com.intel.analytics.bigdl.dlframes.DLClassifier
import com.intel.analytics.bigdl.models.lenet.LeNet5
import com.intel.analytics.bigdl.models.lenet.Utils._
import com.intel.analytics.bigdl.nn.ClassNLLCriterion
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import com.intel.analytics.bigdl.utils.{Engine, File, LoggerFilter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.SQLLeNet5Ext


/**
  * Created by allwefantasy on 28/3/2017.
  */
object Test {
  def main(args: Array[String]): Unit = {

  }
}


object DLClassifierLeNet {
  def load(featureFile: String, labelFile: String): Array[ByteRecord] = {

    val featureBuffer = if (featureFile.startsWith("hdfs:")) {
      ByteBuffer.wrap(File.readHdfsByte(featureFile))
    } else {
      ByteBuffer.wrap(Files.readAllBytes(Paths.get(featureFile)))
    }
    val labelBuffer = if (featureFile.startsWith("hdfs:")) {
      ByteBuffer.wrap(File.readHdfsByte(labelFile))
    } else {
      ByteBuffer.wrap(Files.readAllBytes(Paths.get(labelFile)))
    }
    val labelMagicNumber = labelBuffer.getInt()

    require(labelMagicNumber == 2049)
    val featureMagicNumber = featureBuffer.getInt()
    require(featureMagicNumber == 2051)

    val labelCount = labelBuffer.getInt()
    val featureCount = featureBuffer.getInt()
    require(labelCount == featureCount)

    val rowNum = featureBuffer.getInt()
    val colNum = featureBuffer.getInt()

    val result = new Array[ByteRecord](featureCount)
    var i = 0
    while (i < featureCount) {
      val img = new Array[Byte]((rowNum * colNum))
      var y = 0
      while (y < rowNum) {
        var x = 0
        while (x < colNum) {
          img(x + y * colNum) = featureBuffer.get()
          x += 1
        }
        y += 1
      }
      result(i) = ByteRecord(img, labelBuffer.get().toFloat + 1.0f)
      i += 1
    }

    result
  }

  LoggerFilter.redirectSparkInfoLogs()

  def main(args: Array[String]): Unit = {
    val mlsqlContext = ScriptSQLExec.contextGetOrForTest()
    val conf = Engine.createSparkConf()
      .setAppName("MLPipeline Example")
      .set("spark.task.maxFailures", "1")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqLContext = SQLContext.getOrCreate(sc)
    Engine.init

    val folder = "/Users/allwefantasy/Downloads/mnist"
    val trainData = folder + "/train-images.idx3-ubyte"
    val trainLabel = folder + "/train-labels.idx1-ubyte"
    val validationData = folder + "/t10k-images.idx3-ubyte"
    val validationLabel = folder + "/t10k-labels.idx1-ubyte"

    val trainSet = DataSet.array(load(trainData, trainLabel), sc) ->
      BytesToGreyImg(28, 28) -> GreyImgNormalizer(trainMean, trainStd) -> GreyImgToBatch(1)

    val trainingRDD: RDD[Data[Float]] = trainSet.
      asInstanceOf[DistributedDataSet[MiniBatch[Float]]].data(false).map(batch => {
      val feature = batch.getInput().asInstanceOf[Tensor[Float]]
      val label = batch.getTarget().asInstanceOf[Tensor[Float]]
      Data[Float](feature.storage().array(), label.storage().array())
    })
    val trainingDF = sqLContext.createDataFrame(trainingRDD).toDF("features", "label")

    val bigdl = new SQLLeNet5Ext()
    bigdl.train(trainingDF, "/tmp/jack", Map("fitParam.0.featureSize" -> "[28,28]", "fitParam.0.classNum" -> "10"))


    val validationSet = DataSet.array(load(validationData, validationLabel), sc) ->
      BytesToGreyImg(28, 28) -> GreyImgNormalizer(testMean, testStd) -> GreyImgToBatch(1)

    val validationRDD: RDD[Data[Float]] = validationSet.
      asInstanceOf[DistributedDataSet[MiniBatch[Float]]].data(false).map { batch =>
      val feature = batch.getInput().asInstanceOf[Tensor[Float]]
      val label = batch.getTarget().asInstanceOf[Tensor[Float]]
      Data[Float](feature.storage().array(), label.storage().array())
    }
    val validationDF = sqLContext.createDataFrame(validationRDD).toDF("features", "label")
    val transformed = bigdl.batchPredict(validationDF, "/tmp/jack", Map())
    transformed.show(true)

    val spark = validationDF.sparkSession
    val model = bigdl.load(spark, "/tmp/jack", Map())
    spark.udf.register("jack", bigdl.predict(spark, model, "jack", Map()))
    spark.udf.register("vec_dense", (a: Seq[Double]) => {
      Vectors.dense(a.toArray)
    })
    validationDF.createOrReplaceTempView("data")
    spark.sql("select jack(vec_dense(features)) as p,label from data").show(false)

    sc.stop()

  }
}

private case class Data[T](featureData: Array[T], labelData: Array[T])

object UdfUtils {

  def newInstance(clazz: Class[_]): Any = {
    val constructor = clazz.getDeclaredConstructors.head
    constructor.setAccessible(true)
    constructor.newInstance()
  }

  def getMethod(clazz: Class[_], method: String) = {
    val candidate = clazz.getDeclaredMethods.filter(_.getName == method).filterNot(_.isBridge)
    if (candidate.isEmpty) {
      throw new Exception(s"No method $method found in class ${clazz.getCanonicalName}")
    } else if (candidate.length > 1) {
      throw new Exception(s"Multiple method $method found in class ${clazz.getCanonicalName}")
    } else {
      candidate.head
    }
  }

}

case class VeterxAndGroup(vertexId: VertexId, group: VertexId)
