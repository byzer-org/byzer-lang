package streaming.core.compositor.source

import java.util

import org.apache.log4j.Logger
import org.apache.spark.streaming.TestInputStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.strategy.platform.SparkStreamingRuntime

import scala.collection.JavaConversions._

/**
 * 8/29/16 WilliamZhu(allwefantasy@gmail.com)
 */
class MockScalaMapInStreaming[T] extends Compositor[T] {
  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[MockScalaMapInStreaming[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def data = {
    List(Map("a" -> "yes", "b" -> "no"))

  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val ssc = params.get("_runtime_").asInstanceOf[SparkStreamingRuntime].streamingContext
    List((new TestInputStream[Map[String, String]](ssc, Array(data), 1)).asInstanceOf[T])
  }
}
