package streaming.core.compositor.spark.source

import java.util

import org.apache.log4j.Logger
import org.apache.spark.sql._
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import _root_.streaming.core.CompositorHelper
import _root_.streaming.core.strategy.ParamsValidator

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 30/3/2017.
  */
class MultiSQLSourceCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[MultiSQLSourceCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    _configParams.foreach { sourceConfig =>
      val name = sourceConfig.getOrElse("name", "").toString

      val _cfg = sourceConfig.map(f => (f._1.toString, f._2.toString)).map { f =>
        (f._1, params.getOrElse(s"streaming.sql.source.${name}.${f._1}", f._2).toString)
      }.toMap

      val sourcePath = _cfg("path")
      val df = sparkSession(params).read.format(sourceConfig("format").toString).options(
        (_cfg - "format" - "path" - "outputTable").map(f => (f._1.toString, f._2.toString))).load(sourcePath)
      df.createOrReplaceTempView(_cfg.getOrElse("outputTable", _cfg.getOrElse("outputTableName", "")))

      val cacheKey = "__caches__"

      _cfg.get("cache") match {
        case Some(_cache) =>
          val cache = _cache.toBoolean
          if (cache) {
            df.cache()
            if (!params.containsKey(cacheKey)) {
              params.put(cacheKey, new util.ArrayList[DataFrame]())
            }
            params.get(cacheKey).asInstanceOf[util.List[DataFrame]].add(df)

          }
        case _ =>
      }

    }
    List()
  }
}