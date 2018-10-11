package streaming.dsl.mmlib.algs

import com.intel.analytics.bigdl.dlframes.{DLClassifier, DLModel}
import com.intel.analytics.bigdl.models.lenet.LeNet5
import com.intel.analytics.bigdl.nn.{ClassNLLCriterion, Module}
import net.sf.json.JSONArray
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.bigdl.BigDLFunctions

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class SQLLeNet5Ext(override val uid: String) extends SQLAlg with MllibFunctions with BigDLFunctions with BaseClassification {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    params.get(keepVersion.name).
      map(m => set(keepVersion, m.toBoolean)).
      getOrElse($(keepVersion))

    val eTable = params.get(evaluateTable.name)

    SQLPythonFunc.incrementVersion(path, $(keepVersion))
    val spark = df.sparkSession


    bigDLClassifyTrain[Float](df, path, params, (newFitParam) => {
      val model = LeNet5(classNum = newFitParam("classNum").toInt)
      val criterion = ClassNLLCriterion[Float]()
      val alg = new DLClassifier[Float](model, criterion,
        JSONArray.fromObject(newFitParam("featureSize")).map(f => f.asInstanceOf[Int]).toArray)
      alg
    }, (_model, newFitParam) => {
      eTable match {
        case Some(etable) =>
          val model = _model
          val evaluateTableDF = spark.table(etable)
          val predictions = model.transform(evaluateTableDF)
          multiclassClassificationEvaluate(predictions, (evaluator) => {
            evaluator.setLabelCol(newFitParam.getOrElse("labelCol", "label"))
            evaluator.setPredictionCol("prediction")
          })

        case None => List()
      }
    })

    formatOutput(getModelMetaData(spark, path))
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val models = load(df.sparkSession, path, params).asInstanceOf[ArrayBuffer[DLModel[Float]]]
    models.head.transform(df)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val (bestModelPath, baseModelPath, metaPath) = mllibModelAndMetaPath(path, params, sparkSession)
    val trainParams = sparkSession.read.parquet(metaPath + "/0").collect().head.getAs[Map[String, String]]("trainParams")

    val featureSize = JSONArray.fromObject(trainParams("featureSize")).map(f => f.asInstanceOf[Int]).toArray

    val model = Module.loadModule[Float](bestModelPath(0))
    val dlModel = new DLModel[Float](model, featureSize)
    ArrayBuffer(dlModel)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }
}

case class BigDLDefaultConfig(batchSize: Int = 128, maxEpoch: Int = 1)
