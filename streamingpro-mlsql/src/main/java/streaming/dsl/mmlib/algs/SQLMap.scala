package streaming.dsl.mmlib.algs

import net.sf.json.JSONObject
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StringType
import streaming.dsl.mmlib.SQLAlg

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 16/8/2018.
  */
class SQLMap extends SQLAlg with MllibFunctions with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val res = JSONObject.fromObject(sparkSession.table(path.split("/").last).toJSON.head()).map(f => (f._1.toString(), f._2.toString())).toMap
    res
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val res = _model.asInstanceOf[Map[String, String]]
    val f = (a: String) => {
      res(a)
    }
    UserDefinedFunction(f, StringType, Some(Seq(StringType)))
  }
}
