package streaming.core.compositor.spark.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.common.JSONPath

import scala.collection.JavaConversions._


/**
 * 6/13/16 WilliamZhu(allwefantasy@gmail.com)
 */
class FlatJSONCompositor[T] extends Compositor[T] {

  def jsonKeyPaths = {
    _configParams.map { f =>
      f.map(f => (f._1.asInstanceOf[String], f._2.asInstanceOf[String])).toMap
    }
  }


  protected var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val rdd = middleResult(0).asInstanceOf[RDD[String]]
    val _jsonKeyPaths = jsonKeyPaths
    val newRDD = rdd.flatMap { line =>
      _jsonKeyPaths.map { _jsonKeyPath =>
        var item = Map[String, Any]()

        _jsonKeyPath.foreach { kPath =>

          val key = kPath._1
          val path = kPath._2
          try {
            item = item + (key -> JSONPath.read(line, path).asInstanceOf[Any])
          } catch {
            case e: Exception =>
          }

        }

        item
      }


    }
    List(newRDD.asInstanceOf[T])
  }


}
