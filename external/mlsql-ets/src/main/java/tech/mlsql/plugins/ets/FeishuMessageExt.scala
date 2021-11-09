package tech.mlsql.plugins.ets

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.nio.charset.Charset

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.http.HttpHeaders.{ACCEPT, CONTENT_TYPE}
import org.apache.http.client.fluent.Request
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._
import org.json4s.jackson.JsonMethods._
import streaming.dsl.mmlib.{Code, Doc, MarkDownDoc, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.param.WowParams
import streaming.log.WowLog
import tech.mlsql.common.form.{Extra, FormParams, Text}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.auth.BaseETAuth



/**
 * Sends message to feishu webhook
 */
class FeishuMessageExt(override val uid: String) extends SQLAlg with WowParams with Logging with WowLog with BaseETAuth {
  def this() = this(Identifiable.randomUID("tech.mlsql.plugins.ets.FeishuMessageExt"))
  private val _om = new ObjectMapper()
  _om.registerModule(DefaultScalaModule)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    // Parameter checking
    require(params.contains(text.name), "text is required.")
    require(params.contains(webhook.name), "webhook is required")
    val _webhook = params(webhook.name )
    val _text = params(text.name)
    val spark = df.sparkSession
    import spark.implicits._
    implicit val formats: DefaultFormats = DefaultFormats

    val resp = Request.Post(_webhook)
      .addHeader(CONTENT_TYPE, APPLICATION_JSON.getMimeType)
      .addHeader(ACCEPT, APPLICATION_JSON.getMimeType)
      .body( new StringEntity(write(FeishuMessage(_text)) ) )
      .execute()
    // if statusCode != 200 , HttpResponseException is thrown, see code AbstractResponseHandler.java
    val respStr = resp.returnContent().asString(Charset.forName("UTF-8"))
    // We have to handle two Json schemas
    // {"Extra":null,"StatusCode":0,"StatusMessage":"success"}
    // {"code":19001,"data":{},"msg":"param invalid: incoming webhook access token invalid"}
    val jVal = parse(respStr)
    val result = jVal.extractOpt[FeishuError2] match {
      case Some(FeishuError2(_, 0, _)) => Some(Seq("Succeed").toDF)
      case Some(FeishuError2(_, code, msg)) => Some(Seq(s"Failure ${code} ${msg}").toDF)
      case None => None
    }

    result match {
      case Some(dd) => dd
      // If Json schema is not of FeishuError2
      case None =>
        jVal.extractOpt[FeishuError] match {
          case Some(FeishuError(0, _, _)) => Seq("Succeed").toDF
          case Some(FeishuError(code, _, msg)) => Seq(s"Failed ${code} ${msg}").toDF
          case None => df
        }
    }
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {}

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  final val text: Param[String] = new Param[String](parent = this
    , name = "text"
    , doc = FormParams.toJson( Text(
      name = "text"
      , value = ""
      , extra = Extra(
        doc = "The message to send"
        , label = "text"
        , options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )
      )
    )
    )
  )

  final val webhook: Param[String] = new Param[String](parent = this
    , name = "webhook"
    , doc = FormParams.toJson( Text(
      name = "webhook"
      , value = ""
      , extra = Extra(
        doc = "Feishu webhook"
        , label = "webhook"
        , options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )
      )
    )
    )
  )

  override def doc: Doc = Doc(MarkDownDoc,
    """
      | FeishuMessageExt sends specified text to a feishu webhook.
      | Please follow https://www.feishu.cn/hc/zh-CN/articles/360024984973 to get a webhook
      | Use "load modelExample.`FeishuMessageExt` as output;" to see the codeExample.
    """.stripMargin)

  override def codeExample: Code = Code(SQLCode,
    """
      |```sql
      |## Send feishu message by calling a webhook
      |SELECT "hello" AS id msg table_1;
      |RUN table_1 as FeishuMessageExt.`` where text="hello" AND webhook = "https://open.feishu.cn/open-apis/bot/v2/hook/xxx" as A2;
      |
      |""".stripMargin)

  override def etName: String = "feishuMessageExt"
}
