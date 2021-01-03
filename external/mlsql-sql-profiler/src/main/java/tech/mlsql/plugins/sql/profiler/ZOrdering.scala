package tech.mlsql.plugins.sql.profiler

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.indexer.impl.ZOrderingIndexer
import tech.mlsql.tool.LPUtils

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

    val indexer = new ZOrderingIndexer()
    if(params.contains("sql")){
      val newdf = df.sqlContext.sql(params("sql"))
      val maps = indexer.rewrite(newdf.queryExecution.analyzed,Map())
      println(newdf.queryExecution.analyzed)
      println(maps)
      return newdf
    }


    val newDF = indexer.write(df, params)
    newDF.get
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = ???
}
