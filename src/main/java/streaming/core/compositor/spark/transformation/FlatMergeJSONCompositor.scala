package streaming.core.compositor.spark.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.common.json.parser.JSONParser

import scala.collection.JavaConversions._


/**
 * 6/13/16 WilliamZhu(allwefantasy@gmail.com)
 */
class FlatMergeJSONCompositor[T] extends Compositor[T] {

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

    val rdd = middleResult(0) match {
      case df: DataFrame => df.toJSON
      case rd: Any => rd.asInstanceOf[RDD[String]]
    }
    val _jsonKeyPaths = jsonKeyPaths

    val newRDD = rdd.flatMap { line =>
      JSONParser(line, _jsonKeyPaths.toList)
    }
    List(newRDD.asInstanceOf[T])
  }


}
