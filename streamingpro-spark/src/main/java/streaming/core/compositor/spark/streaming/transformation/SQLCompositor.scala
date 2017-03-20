package streaming.core.compositor.spark.streaming.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


class SQLCompositor[T] extends Compositor[T] with CompositorHelper {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def sql = {
    _configParams.get(0).get("sql") match {
      case a: util.List[String] => Some(a.mkString(" "))
      case a: String => Some(a)
      case _ => None
    }
  }

  def outputTableName = {
    config[String]("outputTableName", _configParams)
  }

  val TABLE = "_table_"
  val FUNC = "_func_"

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    require(sql.isDefined, "please set sql  by variable `sql` in config file")
    val _sql = translateSQL(sql.get, params)
    val _outputTableName = outputTableName


    if (!params.containsKey(TABLE) && !params.containsKey(FUNC)) {
      val func = () => {
        val df = sqlContextHolder(params).sql(_sql)
        df.registerTempTable(_outputTableName.get)
      }
      if (params.containsKey("sqlList")) {
        params.get("sqlList").asInstanceOf[ArrayBuffer[() => Unit]] += func
      } else {
        val sqlList = ArrayBuffer[() => Unit]()
        sqlList += func
        params.put("sqlList", sqlList)
      }

      return middleResult
    }


    if (params.containsKey(TABLE)) {

      //parent compositor is  tableCompositor

      val func = params.get(TABLE).asInstanceOf[(Any) => SQLContext]
      params.put(FUNC, (df: Any) => {
        val sqlContext = func(df)
        val newDF = sqlContext.sql(_sql)
        _outputTableName match {
          case Some(tableName) =>
            newDF.registerTempTable(tableName)
          case None =>
        }
        newDF
      })

    } else {
      // if not ,parent is SQLCompositor
      val func = params.get(FUNC).asInstanceOf[(DataFrame) => DataFrame]
      params.put(FUNC, (rddOrDF: Any) => {
        val oldDF = rddOrDF match {
          case rdd: RDD[String] =>
            val sqlContext = sqlContextHolder(params)
            sqlContext.read.json(rdd)

          case df: DataFrame => df
        }

        val newDF = func(oldDF).sqlContext.sql(_sql)
        _outputTableName match {
          case Some(tableName) =>
            newDF.registerTempTable(tableName)
          case None =>
        }
        newDF
      })
    }
    params.remove(TABLE)

    middleResult
  }
}
