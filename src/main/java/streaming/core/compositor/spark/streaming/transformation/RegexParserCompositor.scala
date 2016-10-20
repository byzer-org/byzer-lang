package streaming.core.compositor.spark.streaming.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.common.regex.parser.RegexParser
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._


/**
  *  10/20/16  sunbiaobiao(1319027852@qq.com)
  */

class RegexParserCompositor[T] extends Compositor[T] with CompositorHelper {

  protected var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  def keys = {
    _configParams(0).get("captureNames").asInstanceOf[Seq[String]]
  }

  def patten = {
    _configParams(0).get("patten").asInstanceOf[String]
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val dstream = middleResult(0).asInstanceOf[DStream[String]]
    val _keys = keys.toArray
    val _patten = patten
    val newDstream = dstream.map { line =>
      RegexParser.parse(line, _patten, _keys)
    }
    List(newDstream.asInstanceOf[T])
  }
}


