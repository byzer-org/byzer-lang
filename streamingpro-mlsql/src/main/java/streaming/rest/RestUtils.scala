package streaming.rest

import net.csdn.common.path.Url
import net.csdn.modules.transport.{DefaultHttpTransportService, HttpTransportService}
import net.csdn.modules.transport.HttpTransportService.SResponse
import org.apache.http.HttpResponse
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.fluent.Request
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils

import scala.collection.JavaConverters._

object RestUtils {
  def httpClientPost(urlString: String, data: Map[String, String]): HttpResponse = {
     val nameValuePairs = data
       .map{ case (name, value) => new BasicNameValuePair(name, value) }.toSeq

    Request.Post(urlString)
      .addHeader("Content-Type", "application/x-www-form-urlencoded")
      .body(new UrlEncodedFormEntity(nameValuePairs.asJava, DefaultHttpTransportService.charset))
      .execute()
      .returnResponse()
  }

  def executeWithRetrying[T](maxTries: Int)(function: => T, checker: T => Boolean, failed: T => Unit): T = {
    Stream.range(0, maxTries)
      .map(i => (i, function))
      // Keep trying until the first success or the retries limit has been reached.
      .find { case (i, result) => checker(result) || {
        if (i == maxTries - 1) {
          failed(result)
          true
        } else {
          false
        }}
      }
      .map(_._2)
      .get
  }

  def convertResponse(httpResponse: HttpResponse, urlString: String): SResponse =
    new HttpTransportService.SResponse(
      httpResponse.getStatusLine.getStatusCode,
      EntityUtils.toString(httpResponse.getEntity, DefaultHttpTransportService.charset),
      new Url(urlString))
}
