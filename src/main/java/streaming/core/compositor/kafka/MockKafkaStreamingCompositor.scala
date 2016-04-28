package streaming.core.compositor.kafka

import java.util

import org.apache.log4j.Logger
import org.apache.spark.streaming.TestInputStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.strategy.platform.SparkStreamingRuntime

import scala.collection.JavaConversions._


class MockKafkaStreamingCompositor[T] extends Compositor[T] {

  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[MockKafkaStreamingCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  private def getKafkaParams = {
    _configParams.get(0).filter {
      f =>
        if (f._1 == "topics") false else true
    }.toMap.asInstanceOf[Map[String, String]]
  }

  private def getTopics = {
    _configParams.get(0).get("topics").asInstanceOf[String].split(",").toSet
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val item = s"""{"a":"1","b":"1"}"""
    val item2 = s"""{"a":"2","b":"2"}"""
    val item3 = s"""{"a":"3","b":"3"}"""
    val ssc = params.get("_runtime_").asInstanceOf[SparkStreamingRuntime].streamingContext
    List((new TestInputStream[String](ssc, Seq(Seq(item), Seq(item2), Seq(item3)), 1)).asInstanceOf[T])
  }

}
