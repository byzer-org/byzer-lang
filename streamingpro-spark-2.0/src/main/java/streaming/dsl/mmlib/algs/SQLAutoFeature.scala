package streaming.dsl.mmlib.algs

import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 2/5/2018.
  */
class SQLAutoFeature extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {

  }

  override def load(sparkSession: SparkSession, path: String): Any = {
    null
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
    null
  }
}

class StringFeature {

  def tfidf(str: String, df: DataFrame, path: String, params: Map[String, String]) = {

    val dir = s"/tmp/streamingpro_mlsql_tmp/autofeature/${UUID.randomUUID().toString()}"
    val script =
      s"""
         |
         |train newdata as TokenAnalysis.`${dir}` where
         |`dic.paths`=""
         |and idCol="id"
         |and inputCol="words";
         |
         |load parquet.`${dir}` as tb;
         |
       """
  }

  def word2vec(str: String) = {

  }
}
