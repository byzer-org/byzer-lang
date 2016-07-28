package org.apache.spark.ml.algs

import org.apache.spark.ml.BaseAlgorithmTransformer
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.DataFrame

/**
 * 7/28/16 WilliamZhu(allwefantasy@gmail.com)
 */
class LogicRegressionTransformer(path: String)extends BaseAlgorithmTransformer {

  val model = LogisticRegressionModel.load(path)

  def transform(df: DataFrame): DataFrame = {
    model.transform(df)
  }

}
