package streaming.dsl.mmlib.algs

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.mmlib.SQLAlg

class SQLAutoIncrementKeyExt extends SQLAlg{
  override def train(df: DataFrame, path: String, params: Map[String, String]):DataFrame ={
  val schema:StructType = df.schema.add(StructField("id",LongType))
  val dfRDD: RDD[(Row, Long)] = df.rdd.zipWithUniqueId()
  val rowRDD: RDD[Row] = dfRDD.map(tp => Row.merge(tp._1, Row(tp._2)))
  val result = df.sparkSession.createDataFrame(rowRDD, schema)
   result
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]):Any= throw new RuntimeException(s"${getClass.getName} not support load function.")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]):UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }
  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)
}
