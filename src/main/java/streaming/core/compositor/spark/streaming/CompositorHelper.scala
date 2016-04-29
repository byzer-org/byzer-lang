package streaming.core.compositor.spark.streaming

import java.util
import streaming.core.strategy.platform.SparkStreamingRuntime

import scala.collection.JavaConversions._

/**
 * 4/28/16 WilliamZhu(allwefantasy@gmail.com)
 */
trait CompositorHelper {

  def config[T](name: String, _configParams: util.List[util.Map[Any, Any]]) = {
    if (_configParams.size() > 0 && _configParams(0).containsKey(name)) {
      Some(_configParams(0).get(name).asInstanceOf[T])
    } else None
  }

  def sparkStreamingRuntime(params:util.Map[Any,Any]) = {
    params.get("_runtime_").asInstanceOf[SparkStreamingRuntime]
  }

}
