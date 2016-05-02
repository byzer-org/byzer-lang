package streaming.core.compositor.spark.streaming.transformation

import net.sf.json.JSONObject
import streaming.core.compositor.spark.streaming.CompositorHelper

/**
  * @author bbword 2016-05-02
  */
class MapTransformCompositor[T] extends BaseMapCompositor[T, String, (String, Long)] with CompositorHelper {
  override def map: (String) => (String, Long) = {
    line => (line, 1)
  }
}
