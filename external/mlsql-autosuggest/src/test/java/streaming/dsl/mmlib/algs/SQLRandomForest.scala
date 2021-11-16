package streaming.dsl.mmlib.algs

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg

import scala.collection.mutable.ArrayBuffer

/**
 * 16/11/2021 hellozepp(lisheng.zhanglin@163.com)
 */
class SQLRandomForest extends SQLAlg {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {df.sparkSession.emptyDataFrame}

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ArrayBuffer()

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = null
}
