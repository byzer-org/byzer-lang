package org.apache.spark.ml.algs

import org.apache.spark.ml.evaluation.{Evaluator, RegressionEvaluator}
import org.apache.spark.ml.regression.{LinearRegressionModel, LinearRegression}
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.ml.{BaseAlgorithmEstimator, Estimator, Model}
import org.apache.spark.sql.DataFrame

/**
 * 7/27/16 WilliamZhu(allwefantasy@gmail.com)
 */
class LinearRegressionEstimator(training: DataFrame, params: Array[Map[String, Any]]) extends BaseAlgorithmEstimator {

  val lr = new LinearRegression()

  override def name: String = "lr"

  override def fit: Model[_] = {
    val paramGrid = mlParams(params)
    if (params.length <= 1) {
      lr.fit(source(training), paramGrid(0))
    } else {
      val trainValidationSplit = new TrainValidationSplit()
        .setEstimator(lr)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGrid)
        .setTrainRatio(0.8)
      trainValidationSplit.fit(training)
    }

  }

  override def algorithm: Estimator[_] = lr

  override def evaluator: Evaluator = new RegressionEvaluator()

}
