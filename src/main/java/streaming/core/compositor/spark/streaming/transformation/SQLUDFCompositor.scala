package streaming.core.compositor.spark.streaming.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._

/**
 * 7/4/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SQLUDFCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[SQLUDFCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val sc = sparkContext(params)
    val sqlContext = SQLContext.getOrCreate(sc)
    _configParams(0).foreach { f =>
      val objMethod = Class.forName(f._2.toString)
      objMethod.getMethods.foreach { f =>
        f.invoke(null, sqlContext.udf)
      }
    }
    middleResult
  }
}
