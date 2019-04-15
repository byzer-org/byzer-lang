/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
