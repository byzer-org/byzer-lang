package streaming.core.compositor.spark.output

import java.util
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.common.SparkCompatibility
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
    val oldDf = middleResult.get(0).asInstanceOf[DataFrame]
    val func = params.get("_func_").asInstanceOf[(DataFrame) => DataFrame]
    val _resource = if(params.containsKey("streaming.sql.out.path")) params("streaming.sql.out.path").toString else path.get
    val _cfg = cfg
    val _mode = if (mode.isDefined) mode.get else "ErrorIfExists"
    val _format = format.get
    try {
      val df = func(oldDf)
      val tempDf = df.write.options(_cfg).mode(SaveMode.valueOf(_mode)).format(_format)

      if (SparkCompatibility.sparkVersion.startsWith("1.6") && _format == "jdbc") {
        val properties = new Properties()
        _cfg.foreach(kv => properties.put(kv._1, kv._2))
        tempDf.jdbc(_cfg("url"), _cfg("dbtable"), properties)
      }
      if(_resource=="-"||_resource.isEmpty){
        tempDf.save()
      } else tempDf.save(_resource)

    } catch {
      case e: Exception => e.printStackTrace()
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
