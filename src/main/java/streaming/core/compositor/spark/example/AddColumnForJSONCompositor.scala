package streaming.core.compositor.spark.example

import net.sf.json.JSONObject
import streaming.core.compositor.spark.transformation.BaseMapCompositor

/**
 * 4/28/16 WilliamZhu(allwefantasy@gmail.com)
 */
class AddColumnForJSONCompositor[T] extends BaseMapCompositor[T, String, String] {
  override def map: (String) => String = {
    (line: String) => {
      val res = JSONObject.fromObject(line)
      res.put("d", "1")
      res.toString
    }
  }
}
