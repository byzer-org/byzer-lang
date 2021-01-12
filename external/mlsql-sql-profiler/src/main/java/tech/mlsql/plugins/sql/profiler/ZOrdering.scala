package tech.mlsql.plugins.sql.profiler

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, DataSetHelper, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.indexer.impl.ZOrderingIndexer

/**
 * 31/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ZOrdering(override val uid: String) extends SQLAlg with ETAuth with WowParams {
  def this() = this(WowParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    if (params.contains("schema")) {
      import df.sparkSession.implicits._
      return df.sparkSession.createDataset[String](Seq(df.schema.json)).toDF("value")
    }
    if(!params.contains("indexFields")){
      throw new RuntimeException("indexFields is required")
    }
    val indexer = new ZOrderingIndexer()
    val newDF = indexer.write(df.repartition(params.getOrElse("fileNum", df.rdd.partitions.length + "").toInt), params)
    val temp = newDF.get
    temp
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val indexer = new ZOrderingIndexer()
    val finalLP = indexer.rewrite(df.queryExecution.analyzed, Map())
    val sparkSession = ScriptSQLExec.context().execListener.sparkSession
    val ds = DataSetHelper.create(sparkSession, finalLP)
    ds
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = ???
}
