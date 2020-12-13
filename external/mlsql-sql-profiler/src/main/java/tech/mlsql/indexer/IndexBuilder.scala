package tech.mlsql.indexer

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.ConsoleRequest

/**
 * 7/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class IndexBuilder(override val uid: String) extends SQLAlg with WowParams {
  def this() = this(Identifiable.randomUID("tech.mlsql.indexer.IndexBuilder"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val indexer = params("indexer")
    indexer match {
      case "MySQLIndexer" =>
        ConsoleRequest.execute(params)

    }
    df.sparkSession.emptyDataFrame
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}

