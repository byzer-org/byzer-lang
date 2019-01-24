package streaming.dsl.mmlib.algs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.core.StreamingproJobManager
import streaming.core.datasource.util.MLSQLJobCollect
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-01-11 WilliamZhu(allwefantasy@gmail.com)
  */
class SQLMLSQLJobExt(override val uid: String) extends SQLAlg with WowParams {


  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    val groupId = new MLSQLJobCollect(spark, null).getGroupId(path)
    StreamingproJobManager.killJob(groupId)
    import df.sparkSession.implicits._
    Seq.empty[(String, String)].toDF("param", "description")

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = throw new RuntimeException("register is not support")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = null


}
