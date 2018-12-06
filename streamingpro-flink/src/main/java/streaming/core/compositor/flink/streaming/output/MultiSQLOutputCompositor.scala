package streaming.core.compositor.flink.streaming.output

import java.util

import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sinks.{ConsoleTableSink, CsvTableSink}
import org.apache.log4j.Logger
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.flink.streaming.CompositorHelper
import streaming.core.strategy.ParamsValidator

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 20/3/2017.
  */
class MultiSQLOutputCompositor[T] extends Compositor[T] with CompositorHelper with ParamsValidator {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[MultiSQLOutputCompositor[T]].getName)


  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val tableEnv = params.get("tableEnv").asInstanceOf[TableEnvironment]
    _configParams.foreach { config =>

      val name = config.getOrElse("name", "").toString
      val _cfg = config.map(f => (f._1.toString, f._2.toString)).map { f =>
        (f._1, params.getOrElse(s"streaming.sql.out.${name}.${f._1}", f._2).toString)
      }.toMap

      val tableName = _cfg("inputTableName")
      val options = _cfg - "path" - "mode" - "format"
      val _resource = _cfg.getOrElse("path","-")
      val mode = _cfg.getOrElse("mode", "ErrorIfExists")
      val format = _cfg("format")
      val showNum = _cfg.getOrElse("showNum", "100").toInt


      val ste = tableEnv.asInstanceOf[StreamTableEnvironment]
      format match {
        case "csv" | "com.databricks.spark.csv" =>
          val csvTableSink = new CsvTableSink(_resource)
          ste.scan(tableName).writeToSink(csvTableSink)
        case "console" | "print" =>
          ste.scan(tableName).writeToSink(new ConsoleTableSink(showNum))
        case _ =>
      }
    }

    new util.ArrayList[T]()
  }

  override def valid(params: util.Map[Any, Any]): (Boolean, String) = {
    (true, "")
  }
}


