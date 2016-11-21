package streaming.core.compositor.spark.streaming

import java.util

import streaming.common.SQLContextHolder
import streaming.core.strategy.platform.{SparkStructuredStreamingRuntime, SparkRuntime, SparkStreamingRuntime}

import scala.collection.JavaConversions._

/**
 * 4/28/16 WilliamZhu(allwefantasy@gmail.com)
 */
trait CompositorHelper {

  def config[T](name: String, _configParams: util.List[util.Map[Any, Any]]):Option[T] = {
    config(0, name, _configParams)
  }

  def config[T](index: Int, name: String, _configParams: util.List[util.Map[Any, Any]]) = {
    if (_configParams.size() > 0 && _configParams(0).containsKey(name)) {
      Some(_configParams(index).get(name).asInstanceOf[T])
    } else None
  }

  def sparkStreamingRuntime(params: util.Map[Any, Any]) = {
    params.get("_runtime_").asInstanceOf[SparkStreamingRuntime]
  }

  def sparkRuntime(params: util.Map[Any, Any]) = {
    params.get("_runtime_").asInstanceOf[SparkRuntime]
  }

  def sparkStructuredStreamingRuntime(params: util.Map[Any, Any]) = {
    params.get("_runtime_").asInstanceOf[SparkStructuredStreamingRuntime]
  }

  def sparkContext(params: util.Map[Any, Any]) = {
    params.get("_runtime_") match {
      case a: SparkStreamingRuntime => a.streamingContext.sparkContext
      case b: SparkRuntime => b.sparkContext
      case c: SparkStructuredStreamingRuntime => c.sparkSession.sparkContext
      case _ => throw new RuntimeException("get _runtime_ fail")
    }
  }

  def sqlContextHolder(params: util.Map[Any, Any]) = {
    params.get("_sqlContextHolder_").asInstanceOf[SQLContextHolder].getOrCreate()
  }

  def translateSQL(_sql: String, params: util.Map[Any, Any]) = {
    var sql: String = _sql
    params.filter(_._1.toString.startsWith("streaming.sql.params.")).foreach { p =>
      val key = p._1.toString.split("\\.").last
      sql = sql.replaceAll(":" + key, p._2.toString)
    }
    sql
  }

}
