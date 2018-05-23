package streaming.dsl.mmlib.algs.meta

/**
  * Created by allwefantasy on 22/5/2018.
  */
case class TFIDFMeta(trainParams: Map[String, String], wordIndex: Map[String, Double], tfidfFunc: (Seq[Int] => org.apache.spark.ml.linalg.Vector))
