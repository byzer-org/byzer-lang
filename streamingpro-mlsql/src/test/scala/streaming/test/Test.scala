package streaming.test

import org.apache.http.client.fluent.{Form, Request}
import org.apache.spark.graphx.VertexId


/**
  * Created by allwefantasy on 28/3/2017.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val sql = "select pj(vec_dense(features)) as p1 "

    val res = Request.Post("http://127.0.0.1:9003/model/predict").bodyForm(Form.form().
      add("sql", sql).
      add("data", s"""[{"features":[ 0.045, 8.8, 1.001, 45.0, 7.0, 170.0, 0.27, 0.45, 0.36, 3.0, 20.7 ]}]""").
      add("dataType", "row")
      .build()).execute().returnContent().asString()
    println(res)
  }
}

object UdfUtils {

  def newInstance(clazz: Class[_]): Any = {
    val constructor = clazz.getDeclaredConstructors.head
    constructor.setAccessible(true)
    constructor.newInstance()
  }

  def getMethod(clazz: Class[_], method: String) = {
    val candidate = clazz.getDeclaredMethods.filter(_.getName == method).filterNot(_.isBridge)
    if (candidate.isEmpty) {
      throw new Exception(s"No method $method found in class ${clazz.getCanonicalName}")
    } else if (candidate.length > 1) {
      throw new Exception(s"Multiple method $method found in class ${clazz.getCanonicalName}")
    } else {
      candidate.head
    }
  }

}

case class VeterxAndGroup(vertexId: VertexId, group: VertexId)
