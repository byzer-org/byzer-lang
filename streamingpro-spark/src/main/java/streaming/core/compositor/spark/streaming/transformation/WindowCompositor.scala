package streaming.core.compositor.spark.streaming.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._

/**
  * @author bbword 2016-05-02
  */
class WindowCompositor[T] extends Compositor[T] with CompositorHelper {

  protected var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[WindowCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def windowDuration = {
    val time: Option[Int] = config[Int]("windowDuration", _configParams)
    Seconds(time.get)
  }

  def slideDuration = {
    val time: Option[Int] = config[Int]("slideDuration", _configParams)
    Seconds(time.get)
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val dstream = middleResult(0).asInstanceOf[DStream[T]]
    val newDstream = dstream.window(windowDuration, slideDuration)
    List(newDstream.asInstanceOf[T])
  }
}
