package streaming.dsl.mmlib.algs

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg

import scala.collection.mutable.ArrayBuffer
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLRandomForest(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseClassification {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {


    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
    setKeepVersion(keepVersion)

    val evaluateTable = params.get("evaluateTable")
    setEvaluateTable(evaluateTable.getOrElse("None"))


    SQLPythonFunc.incrementVersion(path, keepVersion)
    val spark = df.sparkSession

    trainModelsWithMultiParamGroup[RandomForestClassificationModel](df, path, params, () => {
      new RandomForestClassifier()
    }, (_model, fitParam) => {
      evaluateTable match {
        case Some(etable) =>
          val model = _model.asInstanceOf[RandomForestClassificationModel]
          val evaluateTableDF = spark.table(etable)
          val predictions = model.transform(evaluateTableDF)
          multiclassClassificationEvaluate(predictions, (evaluator) => {
            evaluator.setLabelCol(fitParam.getOrElse("labelCol", "label"))
            evaluator.setPredictionCol("prediction")
          })

        case None => List()
      }
    }
    )

    formatOutput(getModelMetaData(spark, path))
  }


  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession, () => {
      new RandomForestClassifier()
    })
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {

    val (bestModelPath, baseModelPath, metaPath) = mllibModelAndMetaPath(path, params, sparkSession)
    val model = RandomForestClassificationModel.load(bestModelPath(0))
    ArrayBuffer(model)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    predict_classification(sparkSession, _model, name)
  }

}
