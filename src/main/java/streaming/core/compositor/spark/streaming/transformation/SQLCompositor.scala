package streaming.core.compositor.spark.streaming.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper


class SQLCompositor[T] extends Compositor[T] with CompositorHelper {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def sql = {
    config[String]("sql", _configParams)
  }

  def outputTableName = {
    config[String]("outputTableName", _configParams)
  }

  val TABLE = "_table_"
  val FUNC = "_func_"

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    require(sql.isDefined, "please set sql  by variable `sql` in config file")
    val _sql = sql.get
    if (params.containsKey(TABLE)) {
      //parent compositor is  tableCompositor

      val func = params.get(TABLE).asInstanceOf[(RDD[String]) => SQLContext]
      params.put(FUNC, (rdd: RDD[String]) => {
        val sqlContext = func(rdd)
        val df = sqlContext.sql(_sql)
        outputTableName match {
          case Some(tableName) =>
            val newDf = df.sqlContext.read.json(df.toJSON)
            newDf.registerTempTable(tableName)
          case None =>
        }
        df
      })

    } else {
      // if not ,parent is SQLCompositor
      val func = params.get(FUNC).asInstanceOf[(RDD[String]) => DataFrame]
      params.put(FUNC, (rdd: RDD[String]) => {
        val df = func(rdd)
        outputTableName match {
          case Some(tableName) =>
            val newDf = df.sqlContext.read.json(df.toJSON)
            newDf.registerTempTable(tableName)
          case None =>
        }
        df.sqlContext.sql(_sql)
      })
    }
    params.remove(TABLE)

    middleResult
  }
}
