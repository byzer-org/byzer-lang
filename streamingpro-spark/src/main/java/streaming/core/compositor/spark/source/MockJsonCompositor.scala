package streaming.core.compositor.spark.source

import java.util

import net.sf.json.JSONObject
import org.apache.log4j.Logger
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._

/**
 * 4/29/16 WilliamZhu(allwefantasy@gmail.com)
 */
class MockJsonCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[JDBCCompositor[T]].getName)

  def data = {
    _configParams.map(f => JSONObject.fromObject(f).toString()).toSeq
  }

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val rdd = sparkContext(params).makeRDD[String](data)
    val sqlContext = sqlContextHolder(params)
    val df = sqlContext.read.json(rdd)
    List(df.asInstanceOf[T])
  }
}
