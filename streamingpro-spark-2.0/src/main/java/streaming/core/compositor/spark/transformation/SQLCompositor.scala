package streaming.core.compositor.spark.transformation

import java.util

import org.apache.log4j.Logger
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper

import scala.collection.JavaConversions._


/**
  * Created by allwefantasy on 27/3/2017.
  */
class SQLCompositor[T] extends Compositor[T] with CompositorHelper {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def sql = {
    _configParams.get(0).get("sql") match {
      case a: util.List[String] => Some(a.mkString(" "))
      case a: String => Some(a)
      case _ => None
    }
  }

  def outputTableName = {
    config[String]("outputTableName", _configParams)
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    require(sql.isDefined, "please set sql  by variable `sql` in config file")


    val _sql = translateSQL(sql.get, params)
    val _outputTableName = outputTableName

    val df = sparkSession(params).sql(_sql)

    config[String]("type", _configParams) match {
      case Some(name) if name == "ddl" => df.show(1)
      case _ =>
    }

    _outputTableName match {
      case Some(name) if !name.isEmpty && name != "-" => df.createOrReplaceTempView(name)
      case None =>
    }

    if (middleResult == null) List() else middleResult
  }
}
