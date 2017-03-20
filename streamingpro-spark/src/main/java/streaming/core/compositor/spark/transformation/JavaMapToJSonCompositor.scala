package streaming.core.compositor.spark.transformation

import java.util

import net.sf.json.JSONObject
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import serviceframework.dispatcher.{Compositor, Processor, Strategy}

import scala.collection.JavaConversions._


/**
 * 6/13/16 WilliamZhu(allwefantasy@gmail.com)
 */
class JavaMapToJSonCompositor[T] extends Compositor[T] {

  protected var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val rdd = middleResult(0).asInstanceOf[RDD[String]]
    val newRDD = rdd.map { line =>
      JSONObject.fromObject(line).toString()
    }

    List(newRDD.asInstanceOf[T])
  }


}
