package streaming.crawler


import java.security.cert.X509Certificate

import org.apache.http.client.methods.HttpGet
import org.apache.http.conn.ssl.NoopHostnameVerifier
import org.apache.http.impl.client.HttpClients
import org.apache.http.ssl.{SSLContextBuilder, TrustStrategy}
import org.apache.http.util.EntityUtils
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

/**
  * Created by allwefantasy on 2/4/2018.
  */
object HttpClientCrawler {

  private def client = {
    val acceptingTrustStrategy = new TrustStrategy {
      override def isTrusted(x509Certificates: Array[X509Certificate], s: String): Boolean = true
    }
    val sslContext = new SSLContextBuilder()
      .loadTrustMaterial(null, acceptingTrustStrategy).build();

    val client = HttpClients.custom()
      .setSSLContext(sslContext)
      .setSSLHostnameVerifier(new NoopHostnameVerifier())
      .build()
    client
  }

  val httpclient = client

  def request(url: String): Document = {

    val httpget = new HttpGet(url)
    val response = httpclient.execute(httpget)
    try {
      val entity = response.getEntity
      if (entity != null) {
        Jsoup.parse(EntityUtils.toString(entity))
      } else null
    } finally {
      response.close();
    }
  }

  def main(args: Array[String]): Unit = {
    //println(request("https://www.baidu.com"))
    println(request("http://www.javaroots.com/2017/02/how-to-use-apache-httpclient-45-https.html"))
  }
}
