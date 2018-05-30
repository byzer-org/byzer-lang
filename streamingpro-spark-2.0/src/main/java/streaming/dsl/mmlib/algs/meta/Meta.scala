package streaming.dsl.mmlib.algs.meta

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import streaming.dsl.mmlib.algs.DiscretizerTrainData

/**
  * Created by allwefantasy on 22/5/2018.
  */
case class TFIDFMeta(trainParams: Map[String, String], wordIndex: Map[String, Double], tfidfFunc: (Seq[Int] => org.apache.spark.ml.linalg.Vector))

case class Word2VecMeta(trainParams: Map[String, String], wordIndex: Map[String, Double], predictFunc: ((Seq[String]) => Seq[Seq[Double]]))

case class ScaleMeta(trainParams: Map[String, String], removeOutlierValueFunc: (Double, String) => Double, scaleFunc: Vector => Vector)

case class OutlierValueMeta(fieldName: String, lowerRange: Double, upperRange: Double, quantile: Double)

case class MinMaxValueMeta(fieldName: String, min: Double, max: Double)

case class StandardScalerValueMeta(fieldName: String, mean: Double, std: Double)

//case class DiscretizerMeta(params: Array[DiscretizerTrainData], discretizerFunc: Seq[Double] => Seq[Double])
case class DiscretizerMeta(params: Array[DiscretizerTrainData], discretizerFunc: Seq[Double] => Seq[Double])
