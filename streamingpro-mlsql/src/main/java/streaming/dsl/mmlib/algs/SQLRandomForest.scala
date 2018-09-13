package streaming.dsl.mmlib.algs

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import streaming.dsl.mmlib.SQLAlg

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.param._

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLRandomForest(override val uid: String) extends SQLAlg with MllibFunctions with Functions with Params {

  def this() = this(Identifiable.randomUID("SQLRandomForest"))

  final val evaluateTable: Param[String] = new Param[String](this, "evaluateTable",
    "The table name of test dataset when tranning",
    (value: String) => true)

  final def getEvaluateTable: String = $(evaluateTable)

  def setEvaluateTable(value: String): this.type = set(evaluateTable, value)

  final val keepVersion: BooleanParam = new BooleanParam(this, "keepVersion", "If set true, then every time you run the " +
    "algorithm, it will generate a new directory to save the model.")

  setDefault(keepVersion -> true)

  final def getKeepVersion: Boolean = $(keepVersion)

  def setKeepVersion(value: Boolean): this.type = set(keepVersion, value)


  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {


    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
    setKeepVersion(keepVersion)

    SQLPythonFunc.incrementVersion(path, keepVersion)

    val evaluateTable = params.get("evaluateTable")
    setEvaluateTable(evaluateTable.getOrElse("None"))

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

    val newDF = df.sparkSession.read.parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/0")
    formatOutput(newDF)
  }


  override def explainParams(sparkSession: SparkSession): DataFrame = {
    val model = new RandomForestClassifier()
    val rfcParams2 = this.params.map(this.explainParam).map(f => Row.fromSeq(f.split(":", 2)))
    val rfcParams = model.params.map(model.explainParam).map(f => Row.fromSeq(f.split(":", 2)))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rfcParams2 ++ rfcParams, 1), StructType(Seq(StructField("param", StringType), StructField("description", StringType))))
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {

    val (bestModelPath, baseModelPath, metaPath) = mllibModelAndMetaPath(path, params, sparkSession)
    val model = RandomForestClassificationModel.load(bestModelPath(0))
    ArrayBuffer(model)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    predict_classification(sparkSession, _model, name)
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
}
