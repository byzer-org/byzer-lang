package streaming.core.compositor.spark.streaming.transformation

import org.apache.spark.rdd.RDD
import streaming.core.compositor.spark.streaming.CompositorHelper

/**
  * @author bbword 2016-05-02
  */
class RddTransformCompositor[T] extends BaseTransformCompositor[T, String, (String, Long)] with CompositorHelper {
  override def transform: (RDD[String]) => RDD[(String, Long)] = {
    rdd => rdd.map(x => (x, 1L))
  }
}
