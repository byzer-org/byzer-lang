package streaming.rest

import java.util
import javax.servlet.http.HttpServletResponse

import net.csdn.common.exception.RenderFinish
import net.csdn.modules.http.RestResponse
import net.sf.json.JSONArray

import scala.collection.JavaConversions._

/**
  * Created by xiaguobing on 2016/9/12.
  */
trait CSVRender {
  def renderCSV(restResponse: RestResponse, csvResult: String) = {
    val response: HttpServletResponse = restResponse.httpServletResponse()
    response.reset(); // Reset the response
    response.setContentType("application/octet-stream;charset=gbk")
    response.setHeader("Content-Disposition", "attachment; filename=csvResult.csv")
    response.getOutputStream.write(csvResult.getBytes("gbk"))
    response.getOutputStream.flush()
    response.getOutputStream.close()
    throw new RenderFinish
  }

  def transformJson2Csv(json: String) = {
    val arr: JSONArray = JSONArray.fromObject(json)
    if (!arr.isEmpty) {
      val maps = arr.asInstanceOf[util.List[util.Map[String, Any]]]
      val head: util.Map[String, Any] = maps.head
      val keys: List[String] = head.map(h => h._1).toList
      (List(keys.mkString(",")) ++ maps.map {
        m =>
          keys.map(k => m.getOrElse(k, "")).mkString(",")
      }).mkString("\n")
    } else {
      ""
    }
  }

  def renderJsonAsCsv(restResponse: RestResponse, json: String) = {
    val csvResult: String = transformJson2Csv(json)
    renderCSV(restResponse, csvResult)
  }
}
