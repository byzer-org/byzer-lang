package streaming.dsl.mmlib.algs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 7/5/2018.
  */
class SQLAutoModel extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val sklearn = new SQLSKLearn()

  }

  override def load(sparkSession: SparkSession, path: String): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = ???
}
