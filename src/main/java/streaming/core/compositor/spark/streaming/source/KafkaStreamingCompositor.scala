package streaming.core.compositor.spark.streaming.source

import java.util

import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka.KafkaUtils
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.strategy.platform.SparkStreamingRuntime

import scala.collection.JavaConversions._


class KafkaStreamingCompositor[T] extends Compositor[T] {

  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[KafkaStreamingCompositor[T]].getName)

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
    val runtime = params.get("_runtime_").asInstanceOf[SparkStreamingRuntime]
    val ssc = runtime.streamingContext
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      getKafkaParams,
      getTopics)

    restore(runtime)

    val tempStream = kafkaStream.map(f => f._2)
    List(tempStream.asInstanceOf[T])
  }

  def restore(runtime: SparkStreamingRuntime) = {
    val jobName = runtime.params.get("_client_").asInstanceOf[String]
    runtime.streamingRuntimeInfo.sparkStreamingOperator.directKafkaRecoverSource.restoreJobSate(jobName)
  }

}
