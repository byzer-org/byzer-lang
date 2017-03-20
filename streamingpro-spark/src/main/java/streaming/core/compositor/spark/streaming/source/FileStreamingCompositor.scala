package streaming.core.compositor.spark.streaming.source

import java.util

import org.apache.log4j.Logger
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.strategy.platform.SparkStreamingRuntime

import scala.collection.JavaConversions._


class FileStreamingCompositor[T] extends Compositor[T] {

  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[FileStreamingCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  private def getPath = {
    _configParams.get(0).get("path").asInstanceOf[String]
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val runtime = params.get("_runtime_").asInstanceOf[SparkStreamingRuntime]
    val ssc = runtime.streamingContext
    val tempStream = ssc.textFileStream(getPath)
    List(tempStream.asInstanceOf[T])
  }

}
