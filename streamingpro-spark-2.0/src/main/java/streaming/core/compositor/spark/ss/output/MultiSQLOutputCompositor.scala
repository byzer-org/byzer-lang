package streaming.core.compositor.spark.ss.output

import java.util

import java.util.concurrent.TimeUnit
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.ProcessingTime
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper
import streaming.core.strategy.ParamsValidator

import scala.collection.JavaConversions._

/**
  * 5/11/16 WilliamZhu(allwefantasy@gmail.com)
  */
class MultiSQLOutputCompositor[T] extends Compositor[T] with CompositorHelper with ParamsValidator {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[MultiSQLOutputCompositor[T]].getName)


  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def path = {
    config[String]("path", _configParams)
  }

  def format = {
    config[String]("format", _configParams)
  }

  def mode = {
    config[String]("mode", _configParams)
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    val spark = sparkSession(params)
    _configParams.foreach { config =>

      try {
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

        val dbtable = if (options.containsKey("dbtable")) options("dbtable") else _resource


        var newTableDF = spark.table(tableName)

        if (outputFileNum != -1) {
          newTableDF = newTableDF.repartition(outputFileNum)
        }

        val ssStream = newTableDF.writeStream

        if (_cfg.containsKey("checkpoint")) {
          val checkpointDir = _cfg("checkpoint")
          ssStream.option("checkpointLocation", checkpointDir)
        }
        if (dbtable != null && dbtable != "-") {
          ssStream.option("path", dbtable)
        }
        val query = ssStream.options(options).outputMode(mode).format(format)

        query.trigger(ProcessingTime(_cfg.getOrElse("duration", "10").toInt, TimeUnit.SECONDS)).start()

      } catch {
        case e: Exception => e.printStackTrace()
      }

    }
    spark.streams.awaitAnyTermination()

    new util.ArrayList[T]()
  }

  override def valid(params: util.Map[Any, Any]): (Boolean, String) = {
    (true, "")
  }
}
