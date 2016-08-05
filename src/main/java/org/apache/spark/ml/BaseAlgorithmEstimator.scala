package org.apache.spark.ml

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import streaming.common.CodeTemplateFunctions


/**
 * 7/27/16 WilliamZhu(allwefantasy@gmail.com)
 */
trait BaseAlgorithmEstimator {

  def name: String


  def source(training: DataFrame, vectorSize: Int): DataFrame = {

    //val t = CodeTemplateFunctions.vectorize(vectorSize).asInstanceOf[UserDefinedFunction]
    val t = CodeTemplateFunctions.vectorizeByReflect(vectorSize)

    training.select(
      col("label") cast (org.apache.spark.sql.types.DoubleType),
      t(col("features")) as "features"
    )
  }


  def mlParams(multiParams: Array[Map[String, Any]]): Array[ParamMap] = {
    val paramGrid = new ParamGridBuilder()

    val params = multiParams.flatMap(f => f).groupBy(f => f._1).map(f => (f._1, f._2.map(k => k._2))).toArray

    params.foreach { p =>
      val ab = algorithm.getParam(p._1)
      paramGrid.addGrid(ab, p._2)
    }
    paramGrid.build()
  }

  def algorithm: Estimator[_]

  def evaluator: Evaluator

  def fit: Model[_]
}
