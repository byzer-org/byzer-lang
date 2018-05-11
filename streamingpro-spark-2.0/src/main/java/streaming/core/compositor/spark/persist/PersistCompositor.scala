package streaming.core.compositor.spark.persist

import java.util

import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper

import scala.collection.JavaConversions._

class PersistCompositor[T] extends Compositor[T] with CompositorHelper {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[PersistCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def tableName = {
    config[String]("tableName", _configParams)
  }

  def deserialized = {
    config[Boolean]("serialized", _configParams).getOrElse(false)
  }

  def useDisk = {
    config[Boolean]("disk", _configParams).getOrElse(true)
  }

  def useMemory = {
    config[Boolean]("memory", _configParams).getOrElse(true)
  }

  def replication = {
    config[Int]("replication", _configParams).getOrElse(1)
  }

  def storeLevel = {
    val ms = config[String]("storeLevel", _configParams)
    if (ms.isEmpty) {
      StorageLevel.apply(useDisk, useMemory, deserialized, replication)
    } else {
      StorageLevel.fromString(ms.get.toUpperCase)
    }
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    require(tableName.isDefined, "please set tableName  by variable `tableName` in config file")
    sparkSession(params).table(tableName.get).persist(storeLevel)

    List()
  }
}
