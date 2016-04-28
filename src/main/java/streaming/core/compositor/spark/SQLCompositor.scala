package streaming.core.compositor.spark

import java.util

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import serviceframework.dispatcher.{Compositor, Processor, Strategy}

import scala.collection.JavaConversions._


class SQLCompositor[T] extends Compositor[T] {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def sql = {
    _configParams(0).get("sql").toString
  }

  def outputTable = {
    _configParams(0).get("outputTable").toString
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    var dataFrame: DataFrame = null
    val func = params.get("table").asInstanceOf[(RDD[String]) => SQLContext]
    params.put("sql",(rdd:RDD[String])=>{
      val sqlContext = func(rdd)
      dataFrame = sqlContext.sql(sql)
      dataFrame
    })
    middleResult
  }
}
