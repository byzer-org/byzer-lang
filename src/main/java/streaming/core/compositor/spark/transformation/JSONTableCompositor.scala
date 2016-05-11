package streaming.core.compositor.spark.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper
import streaming.core.strategy.ParamsValidator

import scala.collection.JavaConversions._


class JSONTableCompositor[T] extends Compositor[T] with CompositorHelper with ParamsValidator {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[JSONTableCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  def tableName = {
    config[String]("tableName", _configParams)
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val _tableName = tableName.get
    val rdd = middleResult(0).asInstanceOf[RDD[String]]
    val func = (rdd: RDD[String]) => {
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      val jsonRdd = sqlContext.read.json(rdd)
      jsonRdd.registerTempTable(_tableName)
      sqlContext
    }
    func(rdd)
    middleResult

  }

  override def valid(params: util.Map[Any, Any]): (Boolean, String) = {
    if (tableName.isDefined) (true, "")
    else
      (false, s"Job name = ${params("_client_")}, Compositor=JSONTableCompositor,Message = tableName required in JSONTableCompositor")
  }
}
