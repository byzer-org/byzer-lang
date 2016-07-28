package org.apache.spark.ml.algs

import org.apache.spark.ml.BaseAlgorithmTransformer
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.DataFrame

/**
 * 7/28/16 WilliamZhu(allwefantasy@gmail.com)
 */
class ALSTransformer(path: String,parameters:Map[String,String]) extends BaseAlgorithmTransformer{

  val model = ALSModel.load(path)

  def transform(df: DataFrame): DataFrame = {
    model.transform(df)
    //new MatrixFactorizationModel(model.rank,model.itemFactors,model.userFactors)
  }

}
