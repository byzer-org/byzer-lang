package streaming.core.compositor.spark.streaming.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.common.JSONPath

import scala.collection.JavaConversions._


/**
 * 6/13/16 WilliamZhu(allwefantasy@gmail.com)
 */
class FlatJSONCompositor[T] extends Compositor[T] {

  def jsonKeyPath = {
    _configParams(0).map(f => (f._1.asInstanceOf[String], f._2.asInstanceOf[String])).toMap
  }


  protected var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val dstream = middleResult(0).asInstanceOf[DStream[String]]
    val _jsonKeyPath = jsonKeyPath
    val newDstream = dstream.map { line =>
      _jsonKeyPath.map { kPath =>
        val key = kPath._1
        val path = kPath._2
        val value = JSONPath.read(line, path).asInstanceOf[Any]
        (key, value)
      }
    }
    List(newDstream.asInstanceOf[T])
  }


}
