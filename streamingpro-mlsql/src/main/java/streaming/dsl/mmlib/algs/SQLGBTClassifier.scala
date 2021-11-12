package streaming.dsl.mmlib.algs

import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.regression.BaseRegression
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

import scala.collection.mutable.ArrayBuffer

/**
 *
 * @Author; Andie Huang
 * @Date: 2021/11/4 10:49 上午
 *
 */
class SQLGBTClassifier(override val uid: String) extends SQLAlg with Functions with MllibFunctions with BaseClassification with ETAuth {
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
          multiclassClassificationEvaluate(predictions, evaluator => {
            evaluator.setLabelCol(fitParam.getOrElse("labelCol", "label"))
            evaluator.setPredictionCol("prediction")
          })

        case None => List()
      }
    }
    )

    formatOutput(getModelMetaData(spark, path))
  }

  override def explainModel(sparkSession: SparkSession, path: String, params: Map[String, String]): DataFrame = {
    val models = load(sparkSession, path, params).asInstanceOf[ArrayBuffer[GBTClassificationModel]]
    val rows = models.flatMap { model =>
      val modelParams = model.params.filter(param => model.isSet(param)).map { param =>
        val tmp = model.get(param).get
        val str = if (tmp == null) {
          "null"
        } else tmp.toString
        Seq(("fitParam.[group]." + param.name), str)
      }
      Seq(
        Seq("uid", model.uid),
        Seq("numFeatures", model.numFeatures.toString),
        Seq("getNumTrees", model.getNumTrees.toString),
        Seq("featureImportance", model.featureImportances.toString()),
        Seq("totalNumNodes", model.totalNumNodes.toString())
      ) ++ modelParams
    }.map(Row.fromSeq(_))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows, 1),
      StructType(Seq(StructField("name", StringType), StructField("value", StringType))))
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

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val model = load(df.sparkSession, path, params).asInstanceOf[ArrayBuffer[GBTClassificationModel]].head
    model.transform(df)
  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__algo_gbt_classifier_operator__"),
      OperateType.SELECT,
      Option("select"),
      TableType.SYSTEM)

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getTableAuth match {
      case Some(tableAuth) =>
        tableAuth.auth(List(vtable))
      case None =>
        List(TableAuthResult(granted = true, ""))
    }

  }

  override def doc: Doc = Doc(HtmlDoc,
    """
      | <a href="https://en.wikipedia.org/wiki/Gradient_boosting">Gradient Boosting</a> is a
      | machine learning technique for regression, classification and other tasks,
      | which produces a prediction model in the form of an ensemble of weak prediction models, typically decision trees
      |
      | Use "load modelParams.`GBTClassifier` as output;"
      |
      | to check the available hyper parameters;
      |
      | Use "load modelExample.`GBTClassifier` as output;"
      | get example.
      |
      | If you wanna check the params of model you have trained, use this command:
      |
      | ```
      | load modelExplain.`/tmp/model` where alg="GBTClassifier" as outout;
      | ```
      |
    """.stripMargin)

  override def modelType: ModelType = AlgType


  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |load jsonStr.`jsonStr` as data;
      |select vec_dense(features) as features ,label as label from data
      |as data1;
      |
      |-- use GBTClassifier
      |train data1 as GBTClassifier.`/tmp/model` where
      |
      |-- once set true,every time you run this script, MLSQL will generate new directory for you model
      |keepVersion="true"
      |
      |-- specify the test dataset which will be used to feed evaluator to generate some metrics e.g. F1, Accurate
      |and evaluateTable="data1"
      |
      |-- specify group 0 parameters
      |and `fitParam.0.labelCol`="features"
      |and `fitParam.0.featuresCol`="label"
      |and `fitParam.0.elasticNetParam`="0.1"
      |
      |-- specify group 1 parameters
      |and `fitParam.1.featuresCol`="features"
      |and `fitParam.1.labelCol`="label"
      |and `fitParam.1.elasticNetParam`="0.7"
      |;
    """.stripMargin)
}