package streaming.core.compositor.spark.streaming.output

import java.util
import java.util.concurrent.TimeUnit

import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.streaming.dstream.DStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper
import streaming.core.strategy.ParamsValidator

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class MultiSQLOutputCompositor[T] extends Compositor[T] with CompositorHelper with ParamsValidator {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[MultiSQLOutputCompositor[T]].getName)


  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val dstreams = middleResult.get(0).asInstanceOf[ArrayBuffer[(DStream[(String, String)], String)]]

    val funcs = if (params.containsKey("sqlList")) {
      params.get("sqlList").asInstanceOf[ArrayBuffer[() => Unit]]
    } else ArrayBuffer[() => Unit]()

    val spark = sparkSession(params)

    def output() = {
      _configParams.foreach { config =>

        val name = config.getOrElse("name", "").toString
        val _cfg = config.map(f => (f._1.toString, f._2.toString)).map { f =>
          (f._1, params.getOrElse(s"streaming.sql.out.${name}.${f._1}", f._2).toString)
        }.toMap

        val tableName = _cfg("inputTableName")
        val options = _cfg - "path" - "mode" - "format"
        val _resource = _cfg("path")
        val mode = _cfg.getOrElse("mode", "ErrorIfExists")
        val format = _cfg("format")
        val outputFileNum = _cfg.getOrElse("outputFileNum", "-1").toInt
        val partitionBy = _cfg.getOrElse("partitionBy", "")

        val dbtable = if (options.containsKey("dbtable")) options("dbtable") else _resource

        var newTableDF = spark.table(tableName)

        if (outputFileNum != -1) {
          newTableDF = newTableDF.repartition(outputFileNum)
        }

        if (format == "console") {
          newTableDF.show(_cfg.getOrElse("showNum", "100").toInt)
        } else {

          var tempDf = if (!partitionBy.isEmpty) {
            newTableDF.write.partitionBy(partitionBy.split(","): _*)
          } else {
            newTableDF.write
          }

          tempDf = tempDf.options(options).mode(SaveMode.valueOf(mode)).format(format)

          if (_resource == "-" || _resource.isEmpty) {
            tempDf.save()
          } else tempDf.save(_resource)
        }
      }


    }

    dstreams.foreach { dstreamWithName =>
      val name = dstreamWithName._2
      val dstream = dstreamWithName._1
      dstream.foreachRDD { rdd =>
        import spark.implicits._
        val df = rdd.toDF("id", "content")
        df.createOrReplaceTempView(name)
        funcs.foreach { f =>
          try {
            f()
          } catch {
            case e: Exception =>
              e.printStackTrace()
          }

          output()

        }
      }
    }

    new util.ArrayList[T]()
  }

  override def valid(params: util.Map[Any, Any]): (Boolean, String) = {
    (true, "")
  }
}
