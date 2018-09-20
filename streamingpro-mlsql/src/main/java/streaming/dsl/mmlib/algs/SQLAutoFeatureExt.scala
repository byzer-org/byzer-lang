package streaming.dsl.mmlib.algs

import net.csdn.common.reflect.ReflectHelper
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.log.{Logging, WowLog}

/**
  * Created by allwefantasy on 17/9/2018.
  */
class SQLAutoFeatureExt extends SQLAlg with Logging with WowLog {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val instance = Class.forName("streaming.dsl.mmlib.algs.AutoFeature").newInstance()
    ReflectHelper.method(instance, "train", df, path, params).asInstanceOf[DataFrame]
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val instance = Class.forName("streaming.dsl.mmlib.algs.AutoFeature").newInstance()
    ReflectHelper.method(instance, "batchPredict", df, path, params).asInstanceOf[DataFrame]
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"Register is not support in ${getClass}")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }
}
