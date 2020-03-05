package tech.mlsql.crawler

import java.nio.charset.Charset
import java.security.cert.X509Certificate
import java.util

import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.conn.routing.{HttpRoute, HttpRoutePlanner}
import org.apache.http.conn.ssl.NoopHostnameVerifier
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.http.protocol.HttpContext
import org.apache.http.ssl.{SSLContextBuilder, TrustStrategy}
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHost, HttpRequest}
import tech.mlsql.crawler.beans.WebPage

/**
  * Created by allwefantasy on 2/4/2018.
  */
object HttpClientCrawler {

  private def client(useProxy: Boolean) = {

    val routePlanner = new HttpRoutePlanner() {
      override def determineRoute(target: HttpHost, request: HttpRequest, context: HttpContext): HttpRoute = {

        var proxyStr = ""
        do {
          proxyStr = ProxyUtil.getProxy()
        } while (proxyStr.length == 0)

        val Array(host, port) = proxyStr.split(":")
        return new HttpRoute(target, null, new HttpHost(host, port.toInt),
          "https".equalsIgnoreCase(target.getSchemeName()))
      }
    }

    val acceptingTrustStrategy = new TrustStrategy {
      override def isTrusted(x509Certificates: Array[X509Certificate], s: String): Boolean = true
    }
    val sslContext = new SSLContextBuilder()
      .loadTrustMaterial(null, acceptingTrustStrategy).build();

    var client = HttpClients.custom()
    if (useProxy) {
      client = client.setRoutePlanner(routePlanner)
    }
    client.setSSLContext(sslContext)
      .setSSLHostnameVerifier(new NoopHostnameVerifier())
      .build()
  }

  val httpclient = client(false)
  val httpclientWithpProxy = client(true)

  def request(url: String, useProxy: Boolean = false): WebPage = {

    var response: CloseableHttpResponse = null
    val hc = if (useProxy) httpclientWithpProxy else httpclient
    try {
      val httpget = new HttpGet(url)

      response = hc.execute(httpget)
      val entity = response.getEntity
      if (entity != null) {
        WebPage(EntityUtils.toString(entity), "")
      } else null
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    } finally {
      if (response != null) {
        response.close()
      }

    }
  }

  def requestByMethod(url: String, method: String = "GET", params: Map[String, String], useProxy: Boolean = false): String = {

    var response: CloseableHttpResponse = null
    val hc = if (useProxy) httpclientWithpProxy else httpclient
    try {
      val httpP = method.toLowerCase() match {
        case "get" =>
          val builder = new URIBuilder(url)
          params.foreach(f => builder.setParameter(f._1, f._2))
          new HttpGet(builder.build())
        case "post" =>
          val newParams = new util.ArrayList[BasicNameValuePair]()
          params.foreach(f => newParams.add(new BasicNameValuePair(f._1, f._2)))
          val urlEncodedFormEntity = new UrlEncodedFormEntity(newParams, Charset.forName("utf-8"));
          val httpPost = new HttpPost(url)
          httpPost.setEntity(urlEncodedFormEntity)
          httpPost
      }
      response = hc.execute(httpP)
      val entity = response.getEntity
      if (entity != null) {
        EntityUtils.toString(entity)
      } else null
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        null
    } finally {
      if (response != null) {
        response.close()
      }

    }
  }

  def requestImage(url: String, useProxy: Boolean = false): Array[Byte] = {

    var response: CloseableHttpResponse = null
    val hc = if (useProxy) httpclientWithpProxy else httpclient
    try {
      val httpget = new HttpGet(url)

      response = hc.execute(httpget)
      val entity = response.getEntity
      if (entity != null) {
        EntityUtils.toByteArray(entity)
      } else null
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    } finally {
      if (response != null) {
        response.close()
      }

    }
  }


  def main(args: Array[String]): Unit = {
    //println(request("https://www.baidu.com"))
    println(request("http://www.javaroots.com/2017/02/how-to-use-apache-httpclient-45-https.html"))
  }
}
