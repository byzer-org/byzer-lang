package streaming.core.compositor.spark.streaming.transformation

import streaming.core.compositor.spark.streaming.CompositorHelper

/**
  * @author bbword 2016-05-02
  */
class SumByKeyCompositor[T] extends BaseReduceByKeyCompositor[T, Long] with CompositorHelper {
  override def reduceByKey: (Long, Long) => Long = {
    (x, y) => x + y
  }
}
