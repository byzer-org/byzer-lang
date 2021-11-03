package tech.mlsql.ets.algs

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{lit, regexp_replace, when}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.algs.{CodeExampleText, Functions, MllibFunctions, SQLPythonFunc}
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.dsl.adaptor.MLMapping
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

import scala.collection.mutable.ArrayBuffer

class SQLAutoML(override val uid: String) extends SQLAlg with Functions with MllibFunctions with BaseClassification with ETAuth {

  def this() = this(BaseParams.randomUID())

  final val sortedBy: Param[String] = new Param[String](this, "sortedBy", " ...... ")

  def getAutoMLPath(path: String, algoName: String): String = {
    PathFun.joinPath(path, "__auto_ml__" + algoName)
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
    setKeepVersion(keepVersion)

    val evaluateTable = params.get("evaluateTable")
    setEvaluateTable(evaluateTable.getOrElse("None"))
    val sortedKey = params.getOrElse(sortedBy.name, "f1")
    val algo_list = params.getOrElse("algos", "GBTClassifier,LogisticRegression,NaiveBayes,RandomForest")
      .split(",").map(algo => algo.trim())
    val classifier_list = algo_list.map(algo_name => {
      val tempPath = getAutoMLPath(path, algo_name)
      SQLPythonFunc.incrementVersion(tempPath, keepVersion)
      val sqlAlg = MLMapping.findAlg(algo_name)
      (tempPath.split(PathFun.pathSeparator).last, sqlAlg.train(df, tempPath, params))
    })
    val updatedDF = classifier_list.map(obj => {
      (obj._1, obj._2.withColumn("value", regexp_replace(obj._2("value"),
        PathFun.pathSeparator+"_model_", obj._1 +PathFun.pathSeparator + "_model_")))
    }).map(d => {
      d._2.withColumn("value",
        when(d._2("name") === "algIndex",
          functions.concat(lit(d._1.split(SQLAutoML.pathPrefix).last), lit("."), d._2("value")))
          .otherwise(d._2("value")))
    })
    val classifier_df = updatedDF.map(obj => {
      val metrics = obj.filter(obj("name").equalTo("metrics")).select("value").collectAsList().get(0).get(0)
      val metrics_map = metrics.toString().split("\\n").map(metric => {
        val name = metric.split(":")(0)
        val value = metric.split(":")(1)
        (name, value)
      }).toMap
      val sortedValue = metrics_map.getOrElse(sortedKey, "f1")
      (sortedValue.toFloat, obj)
    }).sortBy(-1 * _._1).map(t => t._2).reduce((x, y) => x.union(y)).toDF()
    classifier_df
  }

  def getBestModelAlgoName(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    var newParam = params
    val algoName = params.get("algIndex") match {
      case Some(algIndex) =>
        newParam = params.updated("algIndex", algIndex.substring(algIndex.indexOf(".") + 1))
        algIndex.substring(0, algIndex.indexOf("."))
      case None =>
        val bestModelPathAmongModels = autoMLfindBestModelPath(path, params, sparkSession)
        bestModelPathAmongModels(0).split("__").last.split(PathFun.pathSeparator)(0)
    }
    (newParam, algoName)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val (newParam, algoName) = getBestModelAlgoName(sparkSession, path, params)
    val bestModelBasePath = getAutoMLPath(path, algoName.asInstanceOf[String])
    val bestModel = MLMapping.findAlg(algoName.asInstanceOf[String]).load(sparkSession, bestModelBasePath, newParam.asInstanceOf[Map[String, String]])
    bestModel
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    predict_classification(sparkSession, _model, name)
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val (newParam, algoName) = getBestModelAlgoName(df.sparkSession, path, params)
    val basModelPath = getAutoMLPath(path, algoName.asInstanceOf[String])
    MLMapping.findAlg(algoName.asInstanceOf[String]).batchPredict(df, basModelPath, newParam.asInstanceOf[Map[String, String]])
  }

  override def explainModel(sparkSession: SparkSession, path: String, params: Map[String, String]): DataFrame = {
    val (newParam, algoName) = getBestModelAlgoName(sparkSession, path, params)
    val basModelPath = getAutoMLPath(path, algoName.asInstanceOf[String])
    MLMapping.findAlg(algoName.asInstanceOf[String]).explainModel(sparkSession, basModelPath, newParam.asInstanceOf[Map[String, String]])
  }

  override def modelType: ModelType = AlgType

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__algo_auto_ml_operator__"),
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
      | AutoML is an extension for finding the best models among GBT, LinearRegression,LogisticRegression,
      | NaiveBayes and RandomForest classifiers.
      |
      | It only supports binary labels and sorted by custmoized performance key.
      |
      | Use "load modelParams.`AutoML` as output;"
      |
      | to check the available parameters;
      |
      | Use "load modelExample.`AutoML` as output;"
      | get example.
      |
      | If you wanna check the params of model you have trained, use this command:
      |
      | ```
      | load modelExplain.`/tmp/model` where alg="AutoML" as outout;
      | ```
      |
    """.stripMargin)

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |load jsonStr.`jsonStr` as data;
      |select vec_dense(features) as features ,label as label from data
      |as data1;
      |
      |-- use AutoML
      |train data1 as AutoML.`/tmp/auto_ml` where
      |
      |-- once set true,every time you run this script, MLSQL will generate new directory for you model
      |algos="LogisticRegression,NaiveBayes"
      |
      |-- specify the metric that sort the trained models e.g. F1, Accurate
      |-- once set true,every time you run this script, MLSQL will generate new directory for you model
      |and keepVersion="true"
      |and evaluateTable="data1"
      |;
    """.stripMargin)
}

object SQLAutoML {
  val pathPrefix: String = "__auto_ml__"
}
