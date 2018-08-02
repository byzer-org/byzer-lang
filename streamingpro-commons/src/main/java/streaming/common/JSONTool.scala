package streaming.common

import net.liftweb.{json => SJSon}
import net.sf.json.{JSONArray, JSONObject}

/**
  * Created by allwefantasy on 2/8/2018.
  */
object JSONTool {
  def parseJson[T](str: String)(implicit m: Manifest[T]) = {
    implicit val formats = SJSon.DefaultFormats
    SJSon.parse(str).extract[T]
  }

  def toJsonStr(item: AnyRef) = {
    implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
    SJSon.Serialization.write(item)
  }

  def toJsonList4J(item: Object) = {
    JSONArray.fromObject(item).toString
  }

  def toJsonMap4J(item: Object) = {
    JSONObject.fromObject(item).toString
  }
}
