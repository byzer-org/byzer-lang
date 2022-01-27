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

package tech.mlsql.crawler.udf

import cn.edu.hfut.dmic.contentextractor.ContentExtractor
import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.entity.ContentType
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntityBuilder}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{DataFrame, UDFRegistration}
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types.StringType
import org.jsoup.Jsoup
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.common.utils.distribute.socket.server.JavaUtils
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.crawler.HttpClientCrawler
import tech.mlsql.crawler.udf.FunctionsUtils.{_http, executeWithRetrying}
import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.tool.{HDFSOperatorV2, Templates2}
import us.codecraft.xsoup.Xsoup

import java.net.URLEncoder
import java.nio.charset.Charset
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 3/4/2018.
  */
object Functions {
  def crawler_auto_extract_body(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("crawler_auto_extract_body", (co: String) => {
      if (co == null) null
      else ContentExtractor.getContentByHtml(co)
    })
  }

  def crawler_auto_extract_title(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("crawler_auto_extract_title", (co: String) => {
      if (co == null) null
      else {
        val doc = Jsoup.parse(co)
        doc.title()
      }

    })
  }

  def crawler_request(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("crawler_request", (co: String) => {
      val docStr = HttpClientCrawler.request(co)
      if (docStr != null) {
        val doc = Jsoup.parse(docStr.pageSource)
        if (doc == null) null
        else
          doc.html()
      } else null
    })
  }


  def crawler_request_image(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("crawler_request_image", (co: String) => {
      val image = HttpClientCrawler.requestImage(co)
      image
    })
  }


  def crawler_http(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("crawler_http", (url: String, method: String, items: Map[String, String]) => {
      HttpClientCrawler.requestByMethod(url, method, items)
    })
  }

  def rest_request(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("rest_request", (url: String, method: String, params: Map[String, String],
                                              headers: Map[String, String], config: Map[String, String]) => {
      val retryInterval = JavaUtils.timeStringAsMs(config.getOrElse("config.retry.interval", "1s"))
      val requestInterval = JavaUtils.timeStringAsMs(config.getOrElse("config.request.interval", "10ms"))
      val maxTries = config.getOrElse("config.retry", "3").toInt
      val (_, content) = executeWithRetrying[(Int, String)](maxTries)((() => {
        try {
          val (status, content) = _http(url, method, params, headers, config)
          Thread.sleep(requestInterval)
          (status, content)
        } catch {
          case e: Exception =>
            val message = "{\"code\":\"500\",\"msg\":\"" + e.getMessage + "\"}"
            (500, message)
        }
      }) (),
        tempResp => {
          val t = tempResp._1 == 200
          if (!t) {
            Thread.sleep(retryInterval)
          }
          t
        },
        failResp => {}
      )
      content
    })
  }

  def crawler_extract_xpath(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("crawler_extract_xpath", (html: String, xpath: String) => {
      if (html == null) null
      else {
        val doc = Jsoup.parse(html)
        doc.title()
        Xsoup.compile(xpath).evaluate(doc).get()
      }

    })
  }

  def crawler_md5(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("crawler_md5", (html: String) => {
      if (html == null) null
      else {
        java.security.MessageDigest.getInstance("MD5").digest(html.getBytes()).map(0xFF & _).map {
          "%02x".format(_)
        }.foldLeft("") {
          _ + _
        }
      }

    })
  }

}

object FunctionsUtils {

  def _http(url: String, method: String, params: Map[String, String],
            headers: Map[String, String], config: Map[String, String]): (Int, String) = {
    val httpMethod = new String(method).toLowerCase()
    val request = httpMethod match {
      case "get" =>

        val finalUrl = if (params.nonEmpty) {
          val urlParam = params.map { case (k, v) => s"$k=$v" }.mkString("&")
          if (url.contains("?")) {
            if (url.endsWith("?"))  url + urlParam else url + "&" + urlParam
          } else {
            url + "?" + urlParam
          }
        } else url

        Request.Get(finalUrl)

      case "post" => Request.Post(url)
      case "put" => Request.Put(url)
      case v =>
        throw new MLSQLException(s"HTTP method $v is not support yet")
    }

    if (config.contains("socket-timeout")) {
      request.socketTimeout(JavaUtils.timeStringAsMs(config("socket-timeout")).toInt)
    }

    if (config.contains("connect-timeout")) {
      request.connectTimeout(JavaUtils.timeStringAsMs(config("connect-timeout")).toInt)
    }

    headers foreach { case (k, v) => request.setHeader(k, v) }
    val contentTypeValue = headers.getOrElse("content-type", headers.getOrElse("Content-Type", "application/x-www-form-urlencoded"))
    request.setHeader("Content-Type", contentTypeValue)

    val response = (httpMethod, contentTypeValue) match {
      case ("get", _) => request.execute()

      case ("post", contentType) if contentType.trim.startsWith("application/json") =>
        if (params.contains("body"))
          request.bodyString(params("body"), ContentType.APPLICATION_JSON).execute()
        else {
          request.execute()
        }

      case ("post", contentType) if contentType.trim.startsWith("application/x-www-form-urlencoded") =>
        val form = Form.form()
        params.foreach { case (k, v) =>
          form.add(k, Templates2.dynamicEvaluateExpression(v, ScriptSQLExec.context().execListener.env().toMap))
        }
        request.bodyForm(form.build(), Charset.forName("utf-8")).execute()

      case ("post", contentType) if contentType.trim.startsWith("multipart/form-data") =>

        val context = ScriptSQLExec.contextGetOrForTest()
        val _filePath = params("file-path")
        val finalPath = new DslTool(){}.resourceRealPath(context.execListener, Option(context.owner), _filePath)

        val inputStream = HDFSOperatorV2.readAsInputStream(finalPath)

        val fileName = params("file-name")

        val entity = MultipartEntityBuilder.create.
          setMode(HttpMultipartMode.BROWSER_COMPATIBLE).
          setCharset(Charset.forName("utf-8")).
          addBinaryBody(fileName, inputStream, ContentType.MULTIPART_FORM_DATA, fileName)

        params.filter(v => v._1 != "file-path" && v._1 != "file-name").foreach { case (k, v) =>
          entity.addTextBody(k, Templates2.dynamicEvaluateExpression(v, ScriptSQLExec.context().execListener.env().toMap))
        }
        request.body(entity.build()).execute()
      case (_, v) =>
        throw new MLSQLException(s"content-type $v  is not support yet")
    }

    val httpResponse = response.returnResponse()
    val status = httpResponse.getStatusLine.getStatusCode
    EntityUtils.consumeQuietly(httpResponse.getEntity)
    val content = if (httpResponse.getEntity != null) EntityUtils.toString(httpResponse.getEntity) else ""
    (status, content)
  }


  def executeWithRetrying[T](maxTries: Int)(function: => T, checker: T => Boolean, failed: T => Unit): T = {
    var result: T = function
    for (i <- 1 until maxTries) {
      result = function
      checker(result) || {
        if (i == maxTries - 1) {
          failed(result)
          true
        } else {
          false
        }
      }
    }
    result
  }
}
