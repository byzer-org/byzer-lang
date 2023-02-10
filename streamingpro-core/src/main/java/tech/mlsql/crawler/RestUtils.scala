package tech.mlsql.crawler

import net.csdn.common.path.Url
import net.csdn.modules.transport.HttpTransportService.SResponse
import net.csdn.modules.transport.{DefaultHttpTransportService, HttpTransportService}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.entity.ContentType
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntityBuilder}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpEntity, HttpResponse}
import streaming.dsl.ScriptSQLExec
import streaming.log.WowLog
import tech.mlsql.common.utils.distribute.socket.server.JavaUtils
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JsonUtils
import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.tool.{HDFSOperatorV2, Templates2}

import java.nio.charset.Charset
import scala.annotation.tailrec
import scala.collection.JavaConversions._

object RestUtils extends Logging with WowLog {
  def httpClientPost(urlString: String, data: Map[String, String], headers: Map[String, String]): HttpResponse = {
    val nameValuePairs = data.map { case (name, value) =>
      new BasicNameValuePair(name, value)
    }.toList

    val req = Request.Post(urlString)
      .addHeader("Content-Type", "application/x-www-form-urlencoded")

    headers foreach { case (name, value) =>
      req.setHeader(name, value)
    }

    req.body(new UrlEncodedFormEntity(nameValuePairs, DefaultHttpTransportService.charset))
      .execute()
      .returnResponse()
  }

  def rest_request_string(url: String, method: String, params: Map[String, String], headers: Map[String, String],
                          config: Map[String, String]): (Int, String) = {
    val (status, _content) = rest_request(url, method, params, headers, config)
    val content = if (_content != null) EntityUtils.toString(_content, DefaultHttpTransportService.charset) else ""
    (status, content)
  }

  def rest_request_binary(url: String, method: String, params: Map[String, String], headers: Map[String, String],
                          config: Map[String, String]): (Int, Array[Byte]) = {
    val (status, _content) = rest_request(url, method, params, headers, config)
    val content = if (_content != null) EntityUtils.toByteArray(_content) else Array[Byte]()
    (status, content)
  }

  def rest_request(url: String, method: String, params: Map[String, String], headers: Map[String, String],
                   config: Map[String, String]): (Int, HttpEntity) = {
    val retryInterval = JavaUtils.timeStringAsMs(config.getOrElse("retry.interval", "1s"))
    val requestInterval = JavaUtils.timeStringAsMs(config.getOrElse("request.interval", "10ms"))
    val maxTries = config.getOrElse("retry", "3").toInt
    val debug = config.getOrElse("debug", "false").toBoolean
    val (status, content) = executeWithRetrying[(Int, HttpEntity)](maxTries)((() => {
      try {
        val (status, content) = _http(url, method, params, headers, config)
        Thread.sleep(requestInterval)
        (status, content)
      } catch {
        case e: Exception =>
          if (debug) {
            val message = "request url: " + url + ", msg:" + ExceptionUtils.getMessage(e) + "\r\n\"stackTrace:" +
              ExceptionUtils.getStackTrace(e)
            logError(format(message))
          }
          (500, null)
      }
    }) (),
      tempResp => {
        val t = tempResp != null && tempResp._1 == 200
        if (!t) {
          Thread.sleep(retryInterval)
        }
        t
      },
      failResp => {}
    )
    (status, content)
  }

  def _http(url: String, method: String, params: Map[String, String],
            headers: Map[String, String], config: Map[String, String]): (Int, HttpEntity) = {
    val httpMethod = new String(method).toLowerCase()
    val context = ScriptSQLExec.contextGetOrForTest()
    val debug = config.getOrElse("debug", "false").toBoolean
    val request = httpMethod match {
      case "get" =>

        val finalUrl = if (params.nonEmpty) {
          val urlParam = params.map { case (k, v) => s"$k=$v" }.mkString("&")
          if (url.contains("?")) {
            if (url.endsWith("?")) url + urlParam else url + "&" + urlParam
          } else {
            url + "?" + urlParam
          }
        } else url

        Request.Get(finalUrl)

      case "post" => Request.Post(url)
      case "put" => Request.Put(url)
      case v =>
        if (debug) {
          logError(format(s"request url: $url, Content-Type $v  is not support yet"))
        }
        return null
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

      case ("post" | "put", contentType) if contentType.trim.startsWith("application/json") =>
        if (params.contains("body")) {
          // The key here is body, and the value is a json string
          request.bodyString(params("body"), ContentType.APPLICATION_JSON).execute()
        } else if (params.nonEmpty) {
          try {
            // Compatible with key and value usage style
            request.bodyString(JsonUtils.toJson(params), ContentType.APPLICATION_JSON).execute()
          } catch {
            case e: Exception =>
              if (debug) {
                logError(format(s"Request url: $url , Content-Type: application/json  catch an Exception!e:" +
                  s" ${ExceptionUtils.getMessage(e)}"))
              }
              return null
          }
        } else {
          request.execute()
        }

      case ("post" | "put", contentType) if contentType.trim.startsWith("application/x-www-form-urlencoded") =>
        val form = Form.form()
        params.foreach { case (k, v) =>
          form.add(k, Templates2.dynamicEvaluateExpression(v, ScriptSQLExec.context().execListener.env().toMap))
        }
        request.bodyForm(form.build(), Charset.forName("utf-8")).execute()

      case ("post" | "put", contentType) if contentType.trim.startsWith("multipart/form-data") =>
        val _filePath = config("file-path")
        val finalPath = new DslTool() {}.resourceRealPath(context.execListener, Option(context.owner), _filePath)

        val inputStream = HDFSOperatorV2.readAsInputStream(finalPath)

        val fileName = config("file-name")

        val entity = MultipartEntityBuilder.create.
          setMode(HttpMultipartMode.BROWSER_COMPATIBLE).
          setCharset(Charset.forName("utf-8")).
          addBinaryBody(fileName, inputStream, ContentType.MULTIPART_FORM_DATA, fileName)

        params.foreach { case (k, v) =>
          entity.addTextBody(k, Templates2.dynamicEvaluateExpression(v, ScriptSQLExec.context().execListener.env().toMap))
        }
        request.body(entity.build()).execute()
      case (_, v) =>
        if (debug) {
          logError(format(s"Request url: $url , Content-Type $v  is not support yet"))
        }
        return null
    }

    val httpResponse = response.returnResponse()
    val status = httpResponse.getStatusLine.getStatusCode
    EntityUtils.consumeQuietly(httpResponse.getEntity)
    (status, httpResponse.getEntity)
  }

  def executeWithRetrying[T](maxTries: Int)(function: => T, checker: T => Boolean, failed: T => Unit): T = {
    _executeWithRetrying[T]( Math.max(maxTries -1, 0), 0)(function, checker, failed)
  }

  @tailrec
  private def _executeWithRetrying[T](maxTries: Int, curTry: Int)(function: => T, checker: T => Boolean, failed: T => Unit): T = {
    assert( curTry <= maxTries )
    val result = function
    if( checker(result ) ) {
      result
    }
    else if( curTry < maxTries ) {
      _executeWithRetrying(maxTries, curTry + 1)(function, checker, failed)
    }
    else {
      failed(result)
      result
    }
  }

  def convertResponse(httpResponse: HttpResponse, urlString: String): SResponse =
    new HttpTransportService.SResponse(
      httpResponse.getStatusLine.getStatusCode,
      EntityUtils.toString(httpResponse.getEntity, DefaultHttpTransportService.charset),
      new Url(urlString))

}
