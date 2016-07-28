package streaming.core.compositor.spark.transformation

import java.util
import java.util.concurrent.atomic.AtomicReference

import org.apache.log4j.Logger
import org.apache.spark.ml.BaseAlgorithmTransformer
import org.apache.spark.sql.DataFrame
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.spark.streaming.CompositorHelper

/**
 * 7/27/16 WilliamZhu(allwefantasy@gmail.com)
 */
class AlgorithmCompositor[T] extends Compositor[T] with CompositorHelper {

  protected var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  val mapping = Map(
    "als" -> "org.apache.spark.ml.algs.ALSTransformer",
    "lr" -> "org.apache.spark.ml.algs.LinearRegressionTransformer",
    "lr2" -> "org.apache.spark.ml.algs.LogicRegressionTransformer"
  )

  val instance = new AtomicReference[Any]()

  def algorithm(path: String) = {
    val clzzName = mapping(config[String]("algorithm", _configParams).get)
    if (instance.get() == null) {
      instance.compareAndSet(null, Class.forName(clzzName).
        getConstructors.head.
        newInstance(path, parameters))
    }
    instance.get()
  }

  def path = {
    config[String]("path", _configParams).get
  }

  def parameters = {
    import scala.collection.JavaConversions._
    (_configParams(0) - "path" - "algorithm" - "outputTableName").map { f =>
      (f._1.toString, f._2.toString)
    }.toMap
  }

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def outputTableName = {
    config[String]("outputTableName", _configParams)
  }

  val TABLE = "_table_"
  val FUNC = "_func_"

  override def result(processors: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    val func = params.get(FUNC).asInstanceOf[(DataFrame) => DataFrame]
    params.put(FUNC, (df: DataFrame) => {
      val newDF = algorithm(path).asInstanceOf[BaseAlgorithmTransformer].transform(func(df))
      outputTableName match {
        case Some(tableName) =>
          newDF.registerTempTable(tableName)
        case None =>
      }
      newDF
    })

    params.remove(TABLE)

    middleResult
  }
}
