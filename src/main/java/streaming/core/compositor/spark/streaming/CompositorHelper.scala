package streaming.core.compositor.spark.streaming

import java.util

import org.apache.spark.sql.SQLContext
import streaming.common.SQLContextHolder
import streaming.core.strategy.platform.{SparkRuntime, SparkStreamingRuntime}

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

  def sparkStreamingRuntime(params: util.Map[Any, Any]) = {
    params.get("_runtime_").asInstanceOf[SparkStreamingRuntime]
  }

  def sparkRuntime(params: util.Map[Any, Any]) = {
    params.get("_runtime_").asInstanceOf[SparkRuntime]
  }

  def sparkContext(params: util.Map[Any, Any]) = {
    params.get("_runtime_") match {
      case a: SparkStreamingRuntime => a.streamingContext.sparkContext
      case b: SparkRuntime => b.sparkContext
      case _ => throw new RuntimeException("get _runtime_ fail")
    }
  }

  def sqlContextHolder(params: util.Map[Any, Any]) = {
    params.get("_sqlContextHolder_").asInstanceOf[SQLContextHolder].getOrCreate()
  }

}
