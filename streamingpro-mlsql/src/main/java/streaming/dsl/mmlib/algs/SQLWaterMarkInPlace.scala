package streaming.dsl.mmlib.algs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by zhuml on 20/8/2018.
  */
class SQLWaterMarkInPlace extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    emptyDataFrame()(df)
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    val inputTable = params.getOrElse("inputTable", _path)
    val eventTimeCol = params.getOrElse("eventTimeCol", "timestamp")
    val delayThreshold = params.getOrElse("delayThreshold", "10 seconds")
    val df = spark.table(inputTable)
    df.withWatermark(eventTimeCol, delayThreshold).createOrReplaceTempView(inputTable)
    null
  }

  override def predict(spark: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }

  override def skipPathPrefix: Boolean = true
}