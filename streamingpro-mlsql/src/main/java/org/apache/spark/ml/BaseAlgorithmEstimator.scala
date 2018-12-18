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

package org.apache.spark.ml

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}


/**
  * 7/27/16 WilliamZhu(allwefantasy@gmail.com)
  */
trait BaseAlgorithmEstimator {

  def name: String


  def source(training: DataFrame, vectorSize: Int): DataFrame = {

    training.schema.fields.find(p => p.name == "features").head.dataType match {
      case a: StringType =>
        val t = udf { (features: String) =>

          if (!features.contains(":")) {
            val v = features.split(",|\\s+").map(_.toDouble)
            Vectors.dense(v)
          } else {
            val v = features.split(",|\\s+").map(_.split(":")).map(f => (f(0).toInt, f(1).toDouble))
            Vectors.sparse(vectorSize, v)
          }
        }

        training.select(
          col("label") cast (DoubleType),
          t(col("features")) as "features"
        )
      case _ =>
        training
    }


  }

  def mlParams(multiParams: Array[Map[String, Any]]): Array[ParamMap] = {
    val paramGrid = new ParamGridBuilder()

    val params = multiParams.flatMap(f => f).groupBy(f => f._1).map(f => (f._1, f._2.map(k => k._2))).toArray

    params.foreach { p =>
      val ab = algorithm.getParam(p._1)
      paramGrid.addGrid(ab, p._2)
    }
    paramGrid.build()
  }

  def algorithm: Estimator[_]

  def evaluator: Evaluator

  def fit: Model[_]
}
