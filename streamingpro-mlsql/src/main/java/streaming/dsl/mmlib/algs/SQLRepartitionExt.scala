package streaming.dsl.mmlib.algs

import org.apache.spark.ml.param.IntParam
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.BaseParams

/**
  * 2019-01-08 WilliamZhu(allwefantasy@gmail.com)
  */
class SQLRepartitionExt(override val uid: String) extends SQLAlg with Functions with BaseParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    params.get(partitionNum.name).map { item =>
      set(partitionNum, item.toInt)
      item
    }.getOrElse {
      throw new MLSQLException(s"${partitionNum.name} is required")
    }
    df.repartition($(partitionNum))
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = throw new MLSQLException("register is not support")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = throw new MLSQLException("register is not support")

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  final val partitionNum: IntParam = new IntParam(this, "partitionNum",
    "")

  override def explainParams(sparkSession: SparkSession): DataFrame = _explainParams(sparkSession)
}
