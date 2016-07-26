package streaming.core.compositor.spark.source

import java.util

import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._

/**
 * 4/29/16 WilliamZhu(allwefantasy@gmail.com)
 */
class HDFSSourceCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[HDFSSourceCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val sc = sparkContext(params)
    val sourcePath = if(params.containsKey("streaming.sql.source.path")) params("streaming.sql.source.path").toString else _configParams(0)("path").toString
    val rdd = sc.textFile(sourcePath)
    List(rdd.asInstanceOf[T])
  }
}
