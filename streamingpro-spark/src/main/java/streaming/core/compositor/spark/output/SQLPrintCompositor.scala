package streaming.core.compositor.spark.output

import java.util

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper
import streaming.core.strategy.ParamsValidator

import scala.collection.JavaConversions._

/**
 * 5/11/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SQLPrintCompositor[T] extends Compositor[T] with CompositorHelper  {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLPrintCompositor[T]].getName)


  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def num = {
    config[Int]("num", _configParams)
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val oldDf = middleResult.get(0).asInstanceOf[DataFrame]
    val func = params.get("_func_").asInstanceOf[(DataFrame) => DataFrame]
    try {
      val df = func(oldDf)
      df.show(num.getOrElse(100))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    params.remove("sql")
    new util.ArrayList[T]()
  }


}
