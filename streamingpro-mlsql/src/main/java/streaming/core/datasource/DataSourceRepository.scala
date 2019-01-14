package streaming.core.datasource

import net.csdn.ServiceFramwork
import net.csdn.common.path.Url
import net.csdn.modules.http.RestRequest
import net.csdn.modules.transport.HttpTransportService
import net.sf.json.{JSONArray, JSONObject}

import scala.collection.JavaConverters._

/**
  * 2019-01-14 WilliamZhu(allwefantasy@gmail.com)
  */
class DataSourceRepository {
  def httpClient = ServiceFramwork.injector.getInstance[HttpTransportService](classOf[HttpTransportService])

  //"http://respository.datasource.mlsql.tech"
  def getOrDefaultUrl = "http://127.0.0.1:8080"

  def listCommand = {
    val res = httpClient.http(new Url(s"${getOrDefaultUrl}/jar/manager/source/mapper"), "{}", RestRequest.Method.POST)

    JSONObject.fromObject(res.getContent).asScala.flatMap { kv =>
      kv._2.asInstanceOf[JSONArray].asScala.map { item =>
        item.asInstanceOf[JSONObject].put("name", kv._1)
        item.toString
      }
    }.toSeq
  }

  def versionCommand:Seq
}
