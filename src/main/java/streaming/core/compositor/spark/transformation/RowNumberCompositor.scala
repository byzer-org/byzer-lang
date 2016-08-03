package streaming.core.compositor.spark.transformation

import java.util

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

import scala.collection.JavaConversions._

/**
 * 8/2/16 WilliamZhu(allwefantasy@gmail.com)
 */
class RowNumberCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    val oldDf = middleResult.get(0).asInstanceOf[DataFrame]

    val df = if (params.containsKey("_func_")) {
      val func = params.get("_func_").asInstanceOf[(DataFrame) => DataFrame]
      func(oldDf)
    } else {
      oldDf
    }

    val newDF = df.sqlContext.createDataFrame(df.rdd.zipWithIndex().map { r =>
      org.apache.spark.sql.Row.fromSeq(
        Seq(r._2) ++ r._1.toSeq
      )
    }, StructType(Array(StructField("label", LongType, false)) ++ df.schema.fields))

    List(newDF.asInstanceOf[T])
  }

}
