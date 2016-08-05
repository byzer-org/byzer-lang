package org.apache.spark.ml.algs

import org.apache.spark.ml.BaseAlgorithmTransformer
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * 7/28/16 WilliamZhu(allwefantasy@gmail.com)
 */
class ALSTransformer(path: String, parameters: Map[String, String]) extends BaseAlgorithmTransformer {

  val model = ALSModel.load(path)
  val matrixFactorizationModel = new MatrixFactorizationModel(model.rank, convertToRDD(model.userFactors), convertToRDD(model.itemFactors))

  def transform(df: DataFrame): DataFrame = {


    if (parameters.contains("recommendUsersForProductsNum")) {
      import df.sqlContext.implicits._
      val dataset = matrixFactorizationModel.recommendUsersForProducts(parameters.get("recommendUsersForProductsNum").
        map(f => f.toInt).getOrElse(10)).toDF("user", "ratings")
      df.join(dataset, dataset("user") === df("user"), "left").
        select(df("user"), df("item"), dataset("ratings")).filter($"ratings".isNotNull)
    } else {
      val newDF = model.transform(df)
      newDF
    }

  }


  private def convertToRDD(dataFrame: DataFrame) = {
    import dataFrame.sqlContext.implicits._
    val res = if (!org.apache.spark.SPARK_REVISION.startsWith("2")) {
      dataFrame.select(dataFrame("id"), dataFrame("features")).map { row =>
        (row.getInt(0), row.getSeq[Float](1).map(_.toDouble).toArray)
      }
    } else {
      val temp = dataFrame.select($"id", $"features").map { row =>
        (row.getInt(0), row.getSeq[Float](1).map(_.toDouble).toArray)
      }
      temp.getClass.getMethod("rdd").invoke(temp)
    }
    res.asInstanceOf[RDD[(Int, Array[Double])]]
  }

}
