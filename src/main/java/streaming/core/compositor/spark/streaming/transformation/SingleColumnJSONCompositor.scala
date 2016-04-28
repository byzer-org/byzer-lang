package streaming.core.compositor.spark.streaming.transformation

import net.sf.json.JSONObject
import streaming.core.compositor.spark.streaming.CompositorHelper

/**
 * 4/28/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SingleColumnJSONCompositor[T] extends BaseMapCompositor[T, String, String] with CompositorHelper {

  def name = {
    config[String]("name", _configParams)
  }

  override def map: (String) => String = {
    require(name.isDefined, "please set column name by variable `name` in config file")
    val _name = name.get
    (line: String) => {
      val res = new JSONObject()
      res.put(_name, line)
      res.toString
    }
  }
}
