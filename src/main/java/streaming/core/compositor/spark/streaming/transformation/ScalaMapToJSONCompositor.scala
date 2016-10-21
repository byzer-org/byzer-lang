package streaming.core.compositor.spark.streaming.transformation

import net.liftweb.{json => SJSon}
import net.sf.json.JSONObject
import streaming.core.compositor.spark.streaming.CompositorHelper

/**
 * 6/2/16 WilliamZhu(allwefantasy@gmail.com)
 */
class ScalaMapToJSONCompositor[T] extends BaseMapCompositor[T, Map[String, String], String] with CompositorHelper {
  override def map: (Map[String, String]) => String = { line =>
    implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
    SJSon.Serialization.write(line)
  }
}

class JavaMapToJSONCompositor[T] extends BaseMapCompositor[T, java.util.Map[String, String], String] with CompositorHelper {
  override def map: (java.util.Map[String, String]) => String = { line =>
    JSONObject.fromObject(line).toString()
  }
}