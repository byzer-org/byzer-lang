package streaming.core.compositor.spark.streaming.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.common.nginx.parser.NginxParser
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._

/**
 * 6/15/16 WilliamZhu(allwefantasy@gmail.com)
 */
class NginxParserCompositor[T] extends Compositor[T] with CompositorHelper {

  protected var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def mapping = {
    _configParams(0).map(f => (f._1.asInstanceOf[String], f._2.toString.toInt)).toMap
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val dstream = middleResult(0).asInstanceOf[DStream[String]]
    val _mapping = mapping
    val newDstream = dstream.map { line =>
      val items = NginxParser.parse(line).toList
      _mapping.map(f => (f._1, items(f._2))).toMap
    }
    List(newDstream.asInstanceOf[T])
  }

}


