package streaming.core.compositor.spark.streaming.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._

/**
  * 4/28/16 WilliamZhu(allwefantasy@gmail.com)
  * 2016-05-02 bbword 1、去掉无用的类型参数；2、修正Logger中的类型
  *
  */
class RepartitionCompositor[T] extends Compositor[T] with CompositorHelper {

  protected var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[RepartitionCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def num = {
    config[Int]("num", _configParams)
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val dstream = middleResult(0).asInstanceOf[DStream[T]]
    val _num = num.get
    val newDstream = dstream.repartition(_num)
    List(newDstream.asInstanceOf[T])
  }

}
