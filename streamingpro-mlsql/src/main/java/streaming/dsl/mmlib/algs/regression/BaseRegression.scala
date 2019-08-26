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

package streaming.dsl.mmlib.algs.regression

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql._
import _root_.streaming.dsl.mmlib.algs.MetricValue
import _root_.streaming.dsl.mmlib.algs.param.BaseParams

trait BaseRegression extends BaseParams {

  def regressionEvaluate(predictions: DataFrame, congigureEvaluator: RegressionEvaluator => Unit) = {
    "rmse|mse|r2|mae".split("\\|").map { metricName =>
      val evaluator = new RegressionEvaluator()
        .setMetricName(metricName)
      congigureEvaluator(evaluator)
      MetricValue(metricName, evaluator.evaluate(predictions))
    }.toList
  }

}
