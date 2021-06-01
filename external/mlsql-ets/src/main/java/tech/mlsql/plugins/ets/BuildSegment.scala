package tech.mlsql.plugins.ets

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.common.utils.log.Logging

class BuildSegment(override val uid: String) extends SQLAlg with WowParams with Logging {

  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.BuildSegment"))

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val jsonObj = new JSONObject
    jsonObj.put("project", params("project"))
    jsonObj.put("start", params("start"))
    jsonObj.put("end", params("end"))
    if (params.contains("sub_partition_values")) {
      jsonObj.put("sub_partition_values", JSON.parseArray(params("sub_partition_values")))
    } else {
      jsonObj.put("sub_partition_values", null)
    }
    if (params.contains("build_all_indexes")) {
      jsonObj.put("build_all_indexes", params("build_all_indexes").toBoolean)
    } else {
      jsonObj.put("build_all_indexes", true)
    }
    if (params.contains("build_all_sub_partitions")) {
      jsonObj.put("build_all_sub_partitions", params("build_all_sub_partitions").toBoolean)
    } else {
      jsonObj.put("build_all_sub_partitions", false)
    }
    if (params.contains("priority")) {
      jsonObj.put("priority", params("priority").toInt)
    } equals {
      jsonObj.put("priority", 3)
    }
    val url = "http://" + params("host") + ":" + params("port") + "/kylin/api/models/" + params("model") + "/segments"
    scheduleAPI(df, params, jsonObj, url)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}
