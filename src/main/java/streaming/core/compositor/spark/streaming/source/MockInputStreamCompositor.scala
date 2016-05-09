package streaming.core.compositor.spark.streaming.source

import java.util

import net.sf.json.JSONArray
import org.apache.log4j.Logger
import org.apache.spark.streaming.TestInputStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.strategy.platform.SparkStreamingRuntime

import scala.collection.JavaConversions._


class MockInputStreamCompositor[T] extends Compositor[T] {

  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[MockInputStreamCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def data = {
    _configParams(0).map(f => f._2.asInstanceOf[JSONArray].map(k => k.asInstanceOf[String]).toSeq).toSeq
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val ssc = params.get("_runtime_").asInstanceOf[SparkStreamingRuntime].streamingContext
    List((new TestInputStream[String](ssc, data, 1)).asInstanceOf[T])
  }
}
