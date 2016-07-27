package org.apache.spark.ml.algs

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.recommendation.{ALSModel, ALS}
import org.apache.spark.ml.{BaseAlgorithmEnhancer, Estimator, Model}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 7/27/16 WilliamZhu(allwefantasy@gmail.com)
 */
class ALSEnhancer(training: DataFrame, params: Array[Map[String, Any]]) extends BaseAlgorithmEnhancer {

  val als: ALS = new ALS()

  override def source(training: DataFrame): DataFrame = {

    val t = udf { features: String =>
      val Array(user, item, rank) = features.split(",")
      Array(user.toInt, item.toInt, rank.toFloat)
    }

    training.select(
      col("label"),
      t(col("features"))(0) as "user",
      t(col("features"))(1) as "item",
      t(col("features"))(2) as "rating"
    )
  }

  override def fit: Model[_] = {
    val paramGrid = mlParams(params)
    als.fit(source(training), paramGrid(0))
  }

  override def algorithm: Estimator[_] = als

  override def evaluator: Evaluator = null

  override def name: String = "als"
}
