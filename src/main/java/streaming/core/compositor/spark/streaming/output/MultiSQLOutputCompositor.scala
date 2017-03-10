package streaming.core.compositor.spark.streaming.output

import java.util
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.dstream.DStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.common.SparkCompatibility
import streaming.core.compositor.spark.streaming.CompositorHelper
import streaming.core.strategy.ParamsValidator

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * 5/11/16 WilliamZhu(allwefantasy@gmail.com)
  */
class MultiSQLOutputCompositor[T] extends Compositor[T] with CompositorHelper with ParamsValidator {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[MultiSQLOutputCompositor[T]].getName)


  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val dstream = middleResult.get(0).asInstanceOf[DStream[(String, String)]]

    val mainStreamFunc = params.get("mainStream").asInstanceOf[(RDD[(String, String)]) => Unit]
    val sqlList = params.getOrElse("sqlList", ArrayBuffer()).asInstanceOf[ArrayBuffer[() => Unit]]
    dstream.foreachRDD { rdd =>

      mainStreamFunc(rdd)
      sqlList.foreach { func => func() }

      _configParams.foreach { config =>

        val name = config.getOrElse("name","").toString
        val _cfg = config.map(f => (f._1.toString, f._2.toString)).map { f =>
          (f._1, params.getOrElse(s"streaming.sql.out.${name}.${f._1}", f._2).toString)
        }.toMap

        val tableName = _cfg("inputTableName")
        val options = _cfg - "path" - "mode" - "format"
        val path = _cfg("path")
        val mode = _cfg.getOrElse("mode", "ErrorIfExists")
        val format = _cfg("format")
        val outputPath = _cfg.getOrElse("outputPath", "streaming.sql.out.path."+_cfg.getOrElse("name",""))
        val sqlContext = sqlContextHolder(params)
        val _resource = if (params.containsKey(outputPath)) params(outputPath).toString else path
        val dbtable = if (options.containsKey("dbtable")) options("dbtable") else _resource
        try {
          val tempDf = sqlContext.table(tableName).write.options(options).mode(SaveMode.valueOf(mode)).format(format)
          if (format == "console") {
            sqlContext.table(tableName).show(_cfg.getOrElse("showNum", "100").toInt)
          } else {
            if (SparkCompatibility.sparkVersion.startsWith("1.6") && format == "jdbc") {
              val properties = new Properties()
              options.foreach(kv => properties.put(kv._1, kv._2))
              tempDf.jdbc(options("url"), dbtable, properties)
            } else {
              if (_resource == "-" || _resource.isEmpty) {
                tempDf.save()
              } else tempDf.save(_resource)
            }
          }

        } catch {
          case e: Exception => e.printStackTrace()
        }


      }
    }

    new util.ArrayList[T]()
  }

  override def valid(params: util.Map[Any, Any]): (Boolean, String) = {
    (true, "")
  }
}
