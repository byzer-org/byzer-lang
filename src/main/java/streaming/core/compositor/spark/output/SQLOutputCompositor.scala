package streaming.core.compositor.spark.output

import java.util

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.streaming.dstream.DStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper
import streaming.core.strategy.ParamsValidator

import scala.collection.JavaConversions._

/**
 * 5/11/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SQLOutputCompositor[T] extends Compositor[T] with CompositorHelper with ParamsValidator {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLOutputCompositor[T]].getName)


  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def path = {
    config[String]("path", _configParams)
  }

  def mode = {
    config[String]("mode", _configParams)
  }

  def format = {
    config[String]("format", _configParams)
  }

  def cfg = {
    val _cfg = _configParams(0).map(f => (f._1.asInstanceOf[String], f._2.asInstanceOf[String])).toMap
    _cfg - "path" - "mode" - "format"
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val dstream = middleResult.get(0).asInstanceOf[DStream[String]]
    val func = params.get("_func_").asInstanceOf[(RDD[String]) => DataFrame]
    val _resource = path.get
    val _cfg = cfg
    val _mode = if (mode.isDefined) mode.get else "ErrorIfExists"
    val _format = format.get
    dstream.foreachRDD { rdd =>
      try {
        val df = func(rdd)
        df.write.options(_cfg).mode(SaveMode.valueOf(_mode)).format(_format).save(_resource)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    params.remove("sql")
    new util.ArrayList[T]()
  }

  override def valid(params: util.Map[Any, Any]): (Boolean, String) = {
    if (path.isDefined) (true, "")
    else
      (false, s"Job name = ${params("_client_")}, Compositor=SQLParquetOutputCompositor,Message = path required")
  }
}
