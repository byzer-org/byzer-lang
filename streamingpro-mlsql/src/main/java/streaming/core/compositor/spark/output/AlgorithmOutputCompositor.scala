package streaming.core.compositor.spark.output

import java.util

import org.apache.log4j.Logger
import org.apache.spark.ml.BaseAlgorithmEstimator
import org.apache.spark.ml.tuning.TrainValidationSplitModel
import org.apache.spark.sql.DataFrame
import serviceframework.dispatcher.{Processor, Strategy}
import streaming.core.compositor.spark.transformation.{BaseAlgorithmCompositor, SQLCompositor}

import scala.collection.JavaConversions._

/**
  * 7/27/16 WilliamZhu(allwefantasy@gmail.com)
  */
class AlgorithmOutputCompositor[T] extends BaseAlgorithmCompositor[T] {


  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  val mapping = Map(
    "als" -> "org.apache.spark.ml.algs.ALSEstimator",
    "lr" -> "org.apache.spark.ml.algs.LinearRegressionEstimator",
    "lr2" -> "org.apache.spark.ml.algs.LogicRegressionEstimator"
  )

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  override def result(processors: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    if (!params.containsKey(FUNC)) {

      try {
        val _inputTableName = inputTableName.get

        val input = sparkSession(params).table(_inputTableName)

        val newParams = _configParams.map(f => f.map(k => (k._1.asInstanceOf[String], k._2)).toMap).toArray
        val bae = algorithm(
          input,
          newParams).
          asInstanceOf[BaseAlgorithmEstimator]
        val model = bae.fit

        model match {
          case a: TrainValidationSplitModel =>
            a.bestModel.getClass.getMethod("save", classOf[String]).invoke(a.bestModel, path)
          case _ => model.getClass.getMethod("save", classOf[String]).invoke(model, path)
        }


      } catch {
        case e: Exception => e.printStackTrace()
      }


    } else {
      val oldDf = middleResult.get(0).asInstanceOf[DataFrame]
      val func = params.get("_func_").asInstanceOf[(DataFrame) => DataFrame]

      try {
        val df = func(oldDf)
        val newParams = _configParams.map(f => f.map(k => (k._1.asInstanceOf[String], k._2)).toMap).toArray
        val bae = algorithm(
          df,
          newParams).
          asInstanceOf[BaseAlgorithmEstimator]
        val model = bae.fit
        model.getClass.getMethod("save", classOf[String]).invoke(model, path)
      } catch {
        case e: Exception => e.printStackTrace()
      }


      params.remove("sql")

    }
    return if (middleResult == null) List() else middleResult


  }
}
