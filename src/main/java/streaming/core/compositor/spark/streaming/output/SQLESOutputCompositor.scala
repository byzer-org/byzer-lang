package streaming.core.compositor.spark.streaming.output

import java.util
import java.util.Locale

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.sql._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper
import streaming.core.strategy.ParamsValidator

import scala.collection.JavaConversions._

/**
 * 5/11/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SQLESOutputCompositor[T] extends Compositor[T] with CompositorHelper with ParamsValidator {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLESOutputCompositor[T]].getName)


  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def resource = {
    val tmp = config[String]("resource", _configParams)
    if (!tmp.isDefined) config[String]("es.resource", _configParams)
    else tmp
  }

  def cfg = {
    val _cfg = _configParams(0).map(f => (f._1.asInstanceOf[String], f._2.asInstanceOf[String])).toMap
    _cfg - "resource" - "timeFormat"

  }


  def timeformat = {
    config[String]("timeFormat", _configParams)
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val dstream = middleResult.get(0).asInstanceOf[DStream[String]]
    val func = params.get("_func_").asInstanceOf[(RDD[String]) => DataFrame]
    val _resource = resource.get
    val _timeFormat = timeformat.getOrElse("")
    val _cfg = cfg
    dstream.foreachRDD { rdd =>
      val df = func(rdd)
      val fmt = DateTimeFormat.forPattern(_timeFormat).withLocale(Locale.ENGLISH)
      val timeStamp = new DateTime(fmt).toString(fmt)
      val finalResource = if (timeformat.isEmpty) _resource else _resource + "_" + timeStamp
      df.saveToEs(finalResource, _cfg)
    }
    params.remove("sql")
    new util.ArrayList[T]()
  }

  override def valid(params: util.Map[Any, Any]): (Boolean, String) = {
    if (resource.isDefined) (true, "")
    else
      (false, s"Job name = ${params("_client_")}, Compositor=SQLESOutputCompositor,Message = resource required")
  }
}
