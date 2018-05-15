package streaming.dsl.mmlib.algs

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.help.HSQLStringIndex
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import streaming.dsl.mmlib.SQLAlg


/**
  * Created by allwefantasy on 2/5/2018.
  */
class SQLAutoFeature extends SQLAlg with Functions {
  //val path = "/tmp/" + UUID.randomUUID().toString
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    null
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }
}


