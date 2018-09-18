package streaming.dsl.mmlib.algs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.log.{Logging, WowLog}

/**
  * Created by allwefantasy on 17/9/2018.
  */
class SQLAutoFeatureExt extends SQLAlg with Logging with WowLog {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    null
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    null
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }
}
