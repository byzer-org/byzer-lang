package org.apache.spark.ml

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 7/27/16 WilliamZhu(allwefantasy@gmail.com)
 */
trait BaseAlgorithmEnhancer {

  def name: String

  def source(training: DataFrame): DataFrame = {

    val t = udf { features: String =>
      val v = features.split(",").map(_.toDouble)
      Vectors.dense(v)
    }

    training.select(
      col("label"),
      t(col("features"))(0) as "features"
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
