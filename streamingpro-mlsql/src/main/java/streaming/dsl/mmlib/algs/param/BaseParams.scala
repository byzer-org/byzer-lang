package streaming.dsl.mmlib.algs.param

import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import streaming.dsl.mmlib.algs.SQLPythonFunc

/**
  * Created by allwefantasy on 14/9/2018.
  */
trait BaseParams extends WowParams {


  final val evaluateTable: Param[String] = new Param[String](this, "evaluateTable",
    "The table name of test dataset when tranning",
    (value: String) => true)


  final def getEvaluateTable: String = $(evaluateTable)

  def setEvaluateTable(value: String): this.type = set(evaluateTable, value)

  final val keepVersion: BooleanParam = new BooleanParam(this, "keepVersion", "If set true, then every time you run the " +
    "algorithm, it will generate a new directory to save the model.")

  setDefault(keepVersion -> true)

  final def getKeepVersion: Boolean = $(keepVersion)

  def setKeepVersion(value: Boolean): this.type = set(keepVersion, value)

  def getModelMetaData(spark: SparkSession, path: String) = {
    spark.read.parquet(SQLPythonFunc.getAlgMetalPath(path, getKeepVersion) + "/0")
  }
}

object BaseParams {
  def randomUID() = {
    Identifiable.randomUID(this.getClass.getName)
  }
}
