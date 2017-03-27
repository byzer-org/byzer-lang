package streaming.core.compositor.spark.ss.source

import java.util

import net.sf.json.JSONArray
import org.apache.log4j.Logger
import org.apache.spark.sql.execution.streaming.MemoryStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper
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

  def data = {
    _configParams(0).map(f => f._2.asInstanceOf[JSONArray].map(k => k.asInstanceOf[String]).toSeq).toSeq
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val sparkSSRt = params.get("_runtime_").asInstanceOf[SparkStructuredStreamingRuntime]
    val ss = sparkSSRt.sparkSessionAdaptor.sparkSession
    import ss.implicits._
    implicit val sqlContext = ss.sqlContext
    val inputData = MemoryStream[String]
    inputData.addData(data.flatMap(f => f).seq)
    val df = inputData.toDS()
    List(df.asInstanceOf[T])
  }
}
