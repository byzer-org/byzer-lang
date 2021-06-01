package tech.mlsql.plugins.ets

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.codec.binary.Base64
import org.apache.http.HttpEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.common.utils.log.Logging

class AutoModel(override val uid: String) extends SQLAlg with WowParams with Logging {

  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.AutoModel"))

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val jsonObj = new JSONObject
    jsonObj.put("project", params("project"))
    val sqls = params("sqls").split("\\;")
    jsonObj.put("sqls", sqls)
    jsonObj.put("with_segment", params("with_segment").toBoolean)
    jsonObj.put("with_model_online", params("with_model_online").toBoolean)
    val url = "http://" + params("host") + ":" + params("port") + "/kylin/api/models/model_suggestion"
    scheduleAPI(df, params, jsonObj, url)
  }
}
