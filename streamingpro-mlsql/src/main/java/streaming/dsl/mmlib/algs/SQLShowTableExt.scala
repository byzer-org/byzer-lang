package streaming.dsl.mmlib.algs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.utils.serder.json.JSONTool

/**
 * 2019-04-11 WilliamZhu(allwefantasy@gmail.com)
 */
class SQLShowTableExt(override val uid: String) extends SQLAlg with Functions with BaseParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    import df.sparkSession.implicits._
    val command = JSONTool.parseJson[List[String]](params("parameters")).toArray
    command match {
      case Array(tableName) => df.sparkSession.sql(s"desc ${tableName}")
      case Array(tableName, "json") => df.sparkSession.createDataset[String](Seq(df.sparkSession.table(tableName).schema.json)).toDF("value")
    }

  }


  override def skipPathPrefix: Boolean = true

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }
}
