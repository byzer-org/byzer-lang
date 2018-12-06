package streaming.dsl.mmlib.algs

import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.Vector
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 15/1/2018.
  */
class SQLGBTs(override val uid: String) extends SQLAlg with Functions with MllibFunctions with BaseClassification {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
    setKeepVersion(keepVersion)

    val evaluateTable = params.get("evaluateTable")
    setEvaluateTable(evaluateTable.getOrElse("None"))


    SQLPythonFunc.incrementVersion(path, keepVersion)
    val spark = df.sparkSession

    trainModelsWithMultiParamGroup[GBTClassificationModel](df, path, params, () => {
      new GBTClassifier()
    }, (_model, fitParam) => {
      evaluateTable match {
        case Some(etable) =>
          val model = _model.asInstanceOf[GBTClassificationModel]
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
      new GBTClassifier()
    })
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val (bestModelPath, baseModelPath, metaPath) = mllibModelAndMetaPath(path, params, sparkSession)
    val model = GBTClassificationModel.load(bestModelPath(0))
    ArrayBuffer(model)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    predict_classification(sparkSession, _model, name)
  }
}
