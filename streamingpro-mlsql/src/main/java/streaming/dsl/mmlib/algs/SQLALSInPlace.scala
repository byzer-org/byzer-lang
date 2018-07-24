package streaming.dsl.mmlib.algs

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import streaming.dsl.mmlib.SQLAlg

import scala.collection.mutable.ArrayBuffer


/**
  * Created by allwefantasy on 24/7/2018.
  */
class SQLALSInPlace extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {

    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
    SQLPythonFunc.incrementVersion(path, keepVersion)

    trainModels[ALSModel](df, SQLPythonFunc.getAlgModelPath(path, keepVersion), params, () => {
      new ALS()
    })

    val model = ALSModel.load(SQLPythonFunc.getAlgModelPath(path, keepVersion) + "/0")

    if (params.contains("userRec")) {
      val userRecs = model.recommendForAllUsers(params.getOrElse("userRec", "10").toInt)
      userRecs.write.mode(SaveMode.Overwrite).parquet(path + "/data/userRec")
    }

    if (params.contains("itemRec")) {
      val itemRecs = model.recommendForAllItems(params.getOrElse("itemRec", "10").toInt)
      itemRecs.write.mode(SaveMode.Overwrite).parquet(path + "/data/itemRec")
    }

    val tempRDD = df.sparkSession.sparkContext.parallelize(Seq(Seq(Map[String, String](), params)), 1).map { f =>
      Row.fromSeq(f)
    }
    df.sparkSession.createDataFrame(tempRDD, StructType(Seq(
      StructField("systemParam", MapType(StringType, StringType)),
      StructField("trainParams", MapType(StringType, StringType))))).
      write.
      mode(SaveMode.Overwrite).
      parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/1")
  }

  override def load(sparkSession: SparkSession, _path: String, params: Map[String, String]): Any = {
    require(params.contains("evaluateTable"), "evaluateTable shoud be configured")

    val maxVersion = SQLPythonFunc.getModelVersion(_path)
    val versionEnabled = maxVersion match {
      case Some(v) => true
      case None => false
    }

    val modelVersion = params.getOrElse("modelVersion", maxVersion.getOrElse(-1).toString).toInt

    // you can specify model version
    val path = if (modelVersion == -1) SQLPythonFunc.getAlgMetalPath(_path, versionEnabled)
    else SQLPythonFunc.getAlgMetalPathWithVersion(_path, modelVersion)

    val modelPath = if (modelVersion == -1) SQLPythonFunc.getAlgModelPath(_path, versionEnabled)
    else SQLPythonFunc.getAlgModelPathWithVersion(_path, modelVersion)

    var algIndex = params.getOrElse("algIndex", "-1").toInt

    val trainParams = sparkSession.read.parquet(path + "/1").collect().map(f => f.getMap[String, String](1)).head.toMap

    val models = new ArrayBuffer[Any]()
    models += ALSModel.load(modelPath + "/0")

    (models, trainParams)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val (models, trainParams) = _model.asInstanceOf[(ArrayBuffer[Any], Map[String, String])]
    val model = models.head.asInstanceOf[ALSModel]
    val evaluateTable = params("evaluateTable")

    val evaluateTableDF = sparkSession.table(evaluateTable)
    val predictions = model.transform(evaluateTableDF)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol(trainParams.getOrElse("ratingCol", "rating"))
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    val f = () => {
      rmse
    }
    UserDefinedFunction(f, DoubleType, Some(Seq()))
  }
}
