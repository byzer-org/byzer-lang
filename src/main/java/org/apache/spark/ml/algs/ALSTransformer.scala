package org.apache.spark.ml.algs

import org.apache.spark.ml.BaseAlgorithmTransformer
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.DataFrame

/**
 * 7/28/16 WilliamZhu(allwefantasy@gmail.com)
 */
class ALSTransformer(path: String, parameters: Map[String, String]) extends BaseAlgorithmTransformer {

  val model = ALSModel.load(path)
  val matrixFactorizationModel = new MatrixFactorizationModel(model.rank, convertToRDD(model.userFactors), convertToRDD(model.itemFactors))

  def transform(df: DataFrame): DataFrame = {
    if(parameters.contains("recommendByUser")){
      matrixFactorizationModel.recommendProducts()
    }
    model.transform(df)
  }

  private def convertToRDD(dataFrame: DataFrame) = {
    dataFrame.select("id", "features").map { case (id: Int, features: Array[Float]) =>
      (id, features.map(_.toDouble).array)
    }
  }

}
