package streaming.core.compositor.spark.persist

import java.util

import org.apache.log4j.Logger
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper

import scala.collection.JavaConversions._

class UnpersistCompositor[T] extends Compositor[T] with CompositorHelper {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[UnpersistCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def tableName = {
    config[String]("tableName", _configParams)
  }

  def blocking = {
    config[Boolean]("blocking", _configParams).getOrElse(false)
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    require(tableName.isDefined, "please set tableName  by variable `tableName` in config file")
    sparkSession(params).table(tableName.get).unpersist(blocking)
    List()
  }
}
