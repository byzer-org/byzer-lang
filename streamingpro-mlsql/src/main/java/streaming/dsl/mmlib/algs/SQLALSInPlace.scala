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

    val evaluateTable = params.get("evaluateTable")
    val sparkSession = df.sparkSession

    trainModelsWithMultiParamGroup[ALSModel](df, path, params, () => {
      new ALS()
    }, (model) => {
      evaluateTable match {
        case Some(etable) =>
          val evaluateTableDF = sparkSession.table(etable)
          val predictions = model.asInstanceOf[ALSModel].transform(evaluateTableDF)
          val evaluator = new RegressionEvaluator()
            .setMetricName("rmse")
            .setLabelCol(params.getOrElse("ratingCol", "rating"))
            .setPredictionCol("prediction")

          val rmse = evaluator.evaluate(predictions)
          //分值越低越好
          -rmse
        case None => 0d
      }
    }
    )



    val maxVersion = SQLPythonFunc.getModelVersion(path)
    val versionEnabled = maxVersion match {
      case Some(v) => true
      case None => false
    }
    val modelVersion = params.getOrElse("modelVersion", maxVersion.getOrElse(-1).toString).toInt

    val modelPath = if (modelVersion == -1) SQLPythonFunc.getAlgModelPath(path, versionEnabled)
    else SQLPythonFunc.getAlgModelPathWithVersion(path, modelVersion)

    val metaPath = if (modelVersion == -1) SQLPythonFunc.getAlgMetalPath(path, versionEnabled)
    else SQLPythonFunc.getAlgMetalPathWithVersion(path, modelVersion)

    var algIndex = params.getOrElse("algIndex", "-1").toInt

    val modelList = sparkSession.read.parquet(metaPath + "/0").collect()
    val models = if (algIndex != -1) {
      Seq(modelPath + "/" + algIndex)
    } else {
      modelList.map(f => (f(3).asInstanceOf[Double], f(0).asInstanceOf[String], f(1).asInstanceOf[Int]))
        .toSeq
        .sortBy(f => f._1)(Ordering[Double].reverse)
        .take(1)
        .map(f => {
          algIndex = f._3
          modelPath + "/" + f._2.split("/").last
        })
    }

    val model = ALSModel.load(models(0))

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
    throw new RuntimeException("register is not supported in ALSInPlace")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException("register is not supported in ALSInPlace")
  }
}
