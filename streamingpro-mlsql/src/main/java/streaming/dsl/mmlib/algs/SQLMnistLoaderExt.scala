package streaming.dsl.mmlib.algs

import java.nio.ByteBuffer

import com.intel.analytics.bigdl.dataset.{ByteRecord, DataSet, DistributedDataSet, MiniBatch}
import com.intel.analytics.bigdl.dataset.image.{BytesToGreyImg, GreyImgNormalizer, GreyImgToBatch}
import com.intel.analytics.bigdl.models.lenet.Utils.{trainMean, trainStd}
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.utils.Engine
import org.apache.spark.ml.param.{BooleanParam, Param}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.common.HDFSOperator
import streaming.dsl.mmlib.{AlgType, ModelType, ProcessType, SQLAlg}
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.session.MLSQLException

class SQLMnistLoaderExt(override val uid: String) extends SQLAlg with BaseParams {

  def this() = this(BaseParams.randomUID())

  def load(featureFile: String, labelFile: String) = {

    val featureBuffer = ByteBuffer.wrap(HDFSOperator.readBytes(featureFile))
    val labelBuffer = ByteBuffer.wrap(HDFSOperator.readBytes(labelFile))

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

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    Engine.init
    params.get(mnistDir.name).
      map(m => set(mnistDir, m)).getOrElse {
      set(mnistDir, path)
      require($(mnistDir) != null, "mnistDir should not empty")
    }



    params.get(dataType.name).
      map(m => set(dataType, m)).
      getOrElse("")

    val trainData = $(mnistDir) + "/train-images.idx3-ubyte"
    val trainLabel = $(mnistDir) + "/train-labels.idx1-ubyte"
    val validationData = $(mnistDir) + "/t10k-images.idx3-ubyte"
    val validationLabel = $(mnistDir) + "/t10k-labels.idx1-ubyte"

    val data = if ($(dataType) == "train") trainData else validationData
    val validate = if ($(dataType) == "train") trainLabel else validationLabel

    val trainSet = DataSet.array(load(data, validate), df.sparkSession.sparkContext) ->
      BytesToGreyImg(28, 28) -> GreyImgNormalizer(trainMean, trainStd) -> GreyImgToBatch(1)

    val trainingRDD: RDD[Data[Float]] = trainSet.
      asInstanceOf[DistributedDataSet[MiniBatch[Float]]].data(false).map(batch => {
      val feature = batch.getInput().asInstanceOf[Tensor[Float]]
      val label = batch.getTarget().asInstanceOf[Tensor[Float]]
      Data[Float](feature.storage().array(), label.storage().array())
    })
    val trainingDF = df.sparkSession.createDataFrame(trainingRDD).toDF("features", "label")
    trainingDF
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def modelType: ModelType = ProcessType

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new MLSQLException(s"register statement is not support by ${getClass.getName}")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  final val mnistDir: Param[String] = new Param[String](this, "mnistDir", "mnistDir directory which contains 4 ubyte files")
  final val dataType: Param[String] = new Param[String](this, "dataType", "load train|validate data")
  set(dataType, "train")
}

private case class Data[T](featureData: Array[T], labelData: Array[T])
