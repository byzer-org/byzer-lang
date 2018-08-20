package streaming.dsl.mmlib.algs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg

/**
 * Created by zhuml on 20/8/2018.
 */
class SQLWaterMarkInPlace extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    val df = spark.sql("select * from " + _path)
    val eventTimeCol = params.getOrElse("eventTimeCol", "timestamp")
    val delayThreshold = params.getOrElse("delayThreshold", "10 seconds")
    df.withWatermark(eventTimeCol, delayThreshold).createOrReplaceTempView(_path)
    null
  }

  override def predict(spark: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }
}