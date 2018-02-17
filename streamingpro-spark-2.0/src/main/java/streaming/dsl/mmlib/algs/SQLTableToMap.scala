package streaming.dsl.mmlib.algs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 8/2/2018.
  */
class SQLTableToMap extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val keyField = params.getOrElse("keyField", "key")
    val valueField = params.getOrElse("keyField", "value")

    val mapResult = df.select(F.col(keyField).as("key"), F.col(valueField).as("value"))
    mapResult.write.mode(SaveMode.Overwrite).parquet(path)

  }

  override def load(sparkSession: SparkSession, path: String): Any = {
    sparkSession.read.parquet(path).collect().map(f => (f.get(0).toString, f.get(1).toString)).toMap
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[Map[String, String]])
    val f = (name: String) => {
      model.value(name)
    }
    UserDefinedFunction(f, StringType, Some(Seq(StringType)))
  }
}
