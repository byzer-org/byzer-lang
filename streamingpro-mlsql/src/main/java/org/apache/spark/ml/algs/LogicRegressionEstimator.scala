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

package org.apache.spark.ml.algs

import org.apache.spark.ml.classification.{LogisticRegressionModel, LogisticRegression}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, Evaluator, RegressionEvaluator}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.ml.{BaseAlgorithmEstimator, Estimator, Model}
import org.apache.spark.sql.DataFrame

/**
  * 7/27/16 WilliamZhu(allwefantasy@gmail.com)
  */
class LogicRegressionEstimator(training: DataFrame, params: Array[Map[String, Any]]) extends BaseAlgorithmEstimator {

   val lr = new LogisticRegression()

   override def name: String = "lr"

   override def fit: Model[_] = {
     val paramGrid = mlParams(params.tail)
     val vectorSize = if (params.head.contains("dicTable")) {
       training.sqlContext.table(params.head.getOrElse("dicTable", "").toString).count()
     } else 0l
     if (params.length <= 1) {
       lr.fit(source(training,vectorSize.toInt), paramGrid(0))
     } else {
       val trainValidationSplit = new TrainValidationSplit()
         .setEstimator(lr)
         .setEvaluator(evaluator)
         .setEstimatorParamMaps(paramGrid)
         .setTrainRatio(0.8)
       trainValidationSplit.fit(source(training,vectorSize.toInt))
     }

   }

   override def algorithm: Estimator[_] = lr

   override def evaluator: Evaluator = new BinaryClassificationEvaluator()

 }
