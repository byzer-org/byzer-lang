package streaming.core.compositor.spark.ss.source

import java.util

import org.apache.log4j.Logger
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper
import streaming.core.strategy.platform.SparkStructuredStreamingRuntime

import scala.collection.JavaConversions._

/**
 * 11/21/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SQLSourceCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[SQLSourceCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val sparkSSRt = sparkRuntime(params).asInstanceOf[SparkStructuredStreamingRuntime]
    val sourcePath = if (params.containsKey("streaming.sql.source.path")) params("streaming.sql.source.path").toString else _configParams(0)("path").toString
    val df = sparkSSRt.sparkSession.readStream.format(_configParams(0)("format").toString).options(
      (_configParams(0) - "format" - "path").map(f => (f._1.asInstanceOf[String], f._2.asInstanceOf[String])).toMap).load(sourcePath)
    List(df.asInstanceOf[T])
  }
}
