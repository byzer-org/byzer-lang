package streaming.core.compositor.spark.streaming.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


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
    val _sql = translateSQL(sql.get, params)
    val _outputTableName = outputTableName

    val func = () => {
      val df = sparkSession(params).sql(_sql)
      df.createOrReplaceTempView(_outputTableName.get)
    }
    if (params.containsKey("sqlList")) {
      params.get("sqlList").asInstanceOf[ArrayBuffer[() => Unit]] += func
    } else {
      val sqlList = ArrayBuffer[() => Unit]()
      sqlList += func
      params.put("sqlList", sqlList)
    }

    middleResult
  }
}
