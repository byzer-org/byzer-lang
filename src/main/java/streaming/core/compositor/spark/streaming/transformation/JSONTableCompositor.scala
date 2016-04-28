package streaming.core.compositor.spark.streaming.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._


class JSONTableCompositor[T] extends Compositor[T] with CompositorHelper{

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[JSONTableCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  def tableName = {
    config[String]("tableName",_configParams)
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val _tableName = tableName.get
    params.put("_table_", (rdd: RDD[String]) => {
      val sqlContext = new SQLContext(rdd.sparkContext)
      val jsonRdd = sqlContext.read.json(rdd)
      jsonRdd.registerTempTable(_tableName)
      sqlContext
    })

    middleResult

  }

}
