package streaming.core.compositor.spark.ss.source

import java.util

import org.apache.log4j.Logger
import org.apache.spark.sql.execution.streaming.MemoryStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper
import streaming.core.strategy.platform.SparkStructuredStreamingRuntime

import scala.collection.JavaConversions._

/**
 * 11/21/16 WilliamZhu(allwefantasy@gmail.com)
 */
class MockSQLSourceCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[MockSQLSourceCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val sparkSSRt = sparkStructuredStreamingRuntime(params)
    val sparkSession = sparkSSRt.sparkSession
    import sparkSession.implicits._
    implicit val sqlContext = sparkSession.sqlContext
    val inputData = MemoryStream[Int]
    inputData.addData(Seq(1, 2, 3, 4))
    val df = inputData.toDS()
    List(df.asInstanceOf[T])
  }
}
