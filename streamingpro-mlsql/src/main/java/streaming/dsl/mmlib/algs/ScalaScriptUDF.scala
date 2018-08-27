package streaming.dsl.mmlib.algs

import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.udf.UDFManager
import streaming.dsl.mmlib.SQLAlg
import streaming.udf.ScalaSourceUDF

/**
  * Created by allwefantasy on 27/8/2018.
  */
class ScalaScriptUDF extends SQLAlg with MllibFunctions with Functions {

  override def skipPathPrefix: Boolean = true

  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {}

  /*
      register ScalaScriptUDF.`scriptText` as udf1;
   */
  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val res = sparkSession.table(path).head().getString(0)
    val methodName = params.get("methodName")
    require(params.contains("methodName"), "methodName is required")
    val (func, returnType) = if (params.contains("className")) {
      ScalaSourceUDF(res, params("className"), methodName)
    } else {
      ScalaSourceUDF(res, methodName)
    }
    UDFManager.register(sparkSession, methodName.get, (e: Seq[Expression]) => ScalaUDF(func, returnType, e))
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }
}
