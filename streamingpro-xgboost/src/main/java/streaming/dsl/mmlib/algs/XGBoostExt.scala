package streaming.dsl.mmlib.algs

import ml.dmlc.xgboost4j.scala.spark.{TrackerConf, WowXGBoostClassifier, XGBoostClassificationModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by allwefantasy on 12/9/2018.
  */
class XGBoostExt {
  def WowXGBoostClassifier = {
    val xgboost = new WowXGBoostClassifier()
    xgboost.set(xgboost.trackerConf, new TrackerConf(0, "scala"))
    xgboost
  }

  def load(tempPath: String) = {
    XGBoostClassificationModel.load(tempPath)
  }

  def explainModel(sparkSession: SparkSession, models: ArrayBuffer[XGBoostClassificationModel]): DataFrame = {
    val rows = models.flatMap { model =>
      val modelParams = model.params.filter(param => model.isSet(param)).map(param =>
        Seq(("fitParam.[group]." + param.name), model.get(param).get.toString))
      Seq() ++ modelParams
    }.map(Row.fromSeq(_))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows, 1),
      StructType(Seq(StructField("name", StringType), StructField("value", StringType))))
  }
}
