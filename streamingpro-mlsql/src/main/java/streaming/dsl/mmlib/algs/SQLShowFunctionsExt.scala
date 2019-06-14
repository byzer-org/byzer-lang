package streaming.dsl.mmlib.algs

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.BaseParams

/**
  * Created by zhuml on 2019/6/14.
  */
class SQLShowFunctionsExt(override val uid: String) extends SQLAlg with Functions with BaseParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    if(StringUtils.isBlank(path)) {
      df.sparkSession.sql(s"show functions")
    } else {
      df.sparkSession.sql(s"desc function ${path}")
    }

  }

  override def skipPathPrefix = true

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]) = throw new RuntimeException(s"${getClass.getName} not support load function.")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]) = throw new RuntimeException(s"${getClass.getName} not support load function.")
}
