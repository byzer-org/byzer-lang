package org.apache.spark.ml.algs

import org.apache.spark.ml.BaseAlgorithmTransformer
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.DataFrame

/**
  * 7/28/16 WilliamZhu(allwefantasy@gmail.com)
  */
class LinearRegressionTransformer(path: String)extends BaseAlgorithmTransformer {

   val model = LinearRegressionModel.load(path)

   def transform(df: DataFrame): DataFrame = {
     model.transform(df)
   }

 }
