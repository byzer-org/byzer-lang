package streaming.core.compositor.spark.source

import java.util

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


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val item = s"""1"""
    val item2 = s"""2"""
    val item3 = s"""3"""
    val ssc = params.get("_runtime_").asInstanceOf[SparkStreamingRuntime].streamingContext
    List((new TestInputStream[String](ssc, Seq(Seq(item), Seq(item2), Seq(item3)), 1)).asInstanceOf[T])
  }

}
