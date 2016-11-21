package streaming.core.compositor.spark.ss.output

import java.util
import java.util.concurrent.TimeUnit

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.ProcessingTime
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

  def format = {
    config[String]("format", _configParams)
  }

  def mode = {
    config[String]("mode", _configParams)
  }

  def cfg = {
    val _cfg = _configParams(0).map(f => (f._1.asInstanceOf[String], f._2.asInstanceOf[String])).toMap
    _cfg - "path" - "mode" - "format"
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val oldDf = middleResult.get(0).asInstanceOf[DataFrame]
    val func = params.get("_func_").asInstanceOf[(DataFrame) => DataFrame]
    val _resource = if (params.containsKey("streaming.sql.out.path")) Some(params("streaming.sql.out.path").toString) else path

    val duration = params.getOrElse("streaming.duration", "10").toString.toInt
    val _mode = if (mode.isDefined) mode.get else "append"


    val _cfg = cfg
    val _format = format.get

    try {
      val df = func(oldDf)
      val query = df.writeStream
      if (params.containsKey("streaming.checkpoint")) {
        val checkpointDir = params.get("streaming.checkpoint").toString
        query.option("checkpointLocation", checkpointDir)
      }

      _resource match {
        case Some(p) => query.option("path", p)
        case None =>
      }

      query.outputMode(_mode)

      val rt = query.trigger(ProcessingTime(duration, TimeUnit.SECONDS)).options(_cfg).format(_format).start()
      rt.awaitTermination()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    params.remove("sql")
    new util.ArrayList[T]()
  }

  override def valid(params: util.Map[Any, Any]): (Boolean, String) = {
    (true, "")
  }
}
