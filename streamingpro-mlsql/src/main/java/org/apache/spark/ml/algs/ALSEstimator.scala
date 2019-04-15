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

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.{BaseAlgorithmEstimator, Estimator, Model}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 7/27/16 WilliamZhu(allwefantasy@gmail.com)
 */
class ALSEstimator(training: DataFrame, params: Array[Map[String, Any]]) extends BaseAlgorithmEstimator {

  val als: ALS = new ALS()

  override def source(training: DataFrame, vectorSize: Int): DataFrame = {

    val t = udf { features: String =>
      val Array(user, item, rank) = features.split(",")
      Array(user.toInt, item.toInt, rank.toFloat)
    }

    training.select(
      col("label"),
      t(col("features"))(0) as "user",
      t(col("features"))(1) as "item",
      t(col("features"))(2) as "rating"
    )
  }

  override def fit: Model[_] = {
    val paramGrid = mlParams(params.tail)
    val vectorSize = if (params.head.contains("dicTable")) {
      training.sqlContext.table(params.head.getOrElse("dicTable", "").toString).count()
    } else 0l

    als.fit(source(training, vectorSize.toInt), paramGrid(0))
  }

  override def algorithm: Estimator[_] = als

  override def evaluator: Evaluator = null

  override def name: String = "als"
}
