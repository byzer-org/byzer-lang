package streaming.dsl.mmlib.algs

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.session.MLSQLException

/*
 *
 * Implement the spark DataFrameNaFunctions function
 * https://spark.apache.org/docs/2.4.0/api/java/org/apache/spark/sql/DataFrameNaFunctions.html
 *
 */

class SQLNaExt(override val uid: String) extends SQLAlg with WowParams {

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val exe = params.get(execute.name).getOrElse(
      "fill"
    )

    if (!execute.isValid(exe)) {
      throw new MLSQLException(s"${execute.name} should be fill or drop or replace")
    }

    val inputTypeParam = params.get(inputType.name).getOrElse(
      "string"
    )
    if (!execute.isValid(inputTypeParam)) {
      throw new MLSQLException(s"${inputType.name} should be int or long or string or double")
    }

    if (exe == "fill") {
      if (inputTypeParam == "long") {
        val value = params.get("value").getOrElse(0L).asInstanceOf[Long]
        df.na.fill(value)
      } else if (inputTypeParam == "double") {
        val value = params.get("value").getOrElse(0.0).asInstanceOf[Double]
        df.na.fill(value)
      } else if (inputTypeParam == "string") {
        val value = params.get("value").getOrElse("").asInstanceOf[String]
        df.na.fill(value)
      }
    } else if (exe == "drop") {
      df.na.drop(params.get("value").getOrElse(""))
    //TODO
    //} else if(exe == "replace") {
      //df.na.replace()
    }

    df
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException("train is not support")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }

  final val execute: Param[String] = new Param[String](this, "execute", "fill|drop", isValid = (m: String) => {
    m == "fill" || m == "drop"
  })

  final val inputType: Param[String] = new Param[String](this, "type", "long|double|string", isValid = (m:String) => {
    m == "long" || m ==" double" ||  m == "string"
  })

  def this() = this(BaseParams.randomUID())
}
