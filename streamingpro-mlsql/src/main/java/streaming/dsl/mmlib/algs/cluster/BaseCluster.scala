package streaming.dsl.mmlib.algs.cluster

import org.apache.spark.ml.evaluation.{ClusteringEvaluator, MulticlassClassificationEvaluator}
import streaming.dsl.mmlib.algs.param.BaseParams
import org.apache.spark.sql._
import _root_.streaming.dsl.mmlib.algs.MetricValue

/**
  * Created by allwefantasy on 14/9/2018.
  */
trait BaseCluster extends BaseParams {
  def clusterEvaluate(predictions: DataFrame, congigureEvaluator: (ClusteringEvaluator) => Unit) = {
    "silhouette".split("\\|").map { metricName =>
      val evaluator = new ClusteringEvaluator()
        .setMetricName(metricName)
      congigureEvaluator(evaluator)
      MetricValue(metricName, evaluator.evaluate(predictions))
    }.toList
  }
}
