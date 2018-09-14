package streaming.dsl.mmlib.algs.classfication

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql._
import _root_.streaming.dsl.mmlib.algs.MetricValue
import _root_.streaming.dsl.mmlib.algs.param.BaseParams

/**
  * Created by allwefantasy on 14/9/2018.
  */
trait BaseClassification extends BaseParams {

  def multiclassClassificationEvaluate(predictions: DataFrame, congigureEvaluator: (MulticlassClassificationEvaluator) => Unit) = {
    "f1|weightedPrecision|weightedRecall|accuracy".split("\\|").map { metricName =>
      val evaluator = new MulticlassClassificationEvaluator()
        .setMetricName(metricName)
      congigureEvaluator(evaluator)
      MetricValue(metricName, evaluator.evaluate(predictions))
    }.toList
  }

}
