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

package ml.dmlc.xgboost4j.scala.spark


import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.FloatType
import ml.dmlc.xgboost4j.{LabeledPoint => XGBLabeledPoint}


class WowXGBoostClassifier extends XGBoostClassifier {

  // called at the start of fit/train when 'eval_metric' is not defined
  private def setupDefaultEvalMetric(): String = {
    require(isDefined(objective), "Users must set \'objective\' via xgboostParams.")
    if ($(objective).startsWith("multi")) {
      // multi
      "merror"
    } else {
      // binary
      "error"
    }
  }

  override protected def train(dataset: Dataset[_]): XGBoostClassificationModel = {

    if (!isDefined(evalMetric) || $(evalMetric).isEmpty) {
      set(evalMetric, setupDefaultEvalMetric())
    }

    val _numClasses = getNumClasses(dataset)
    if (isDefined(numClass) && $(numClass) != _numClasses) {
      throw new Exception("The number of classes in dataset doesn't match " +
        "\'num_class\' in xgboost params.")
    }

    val weight = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val baseMargin = if (!isDefined(baseMarginCol) || $(baseMarginCol).isEmpty) {
      lit(Float.NaN)
    } else {
      col($(baseMarginCol))
    }

    val instances: RDD[XGBLabeledPoint] = dataset.select(
      col($(featuresCol)),
      col($(labelCol)).cast(FloatType),
      baseMargin.cast(FloatType),
      weight.cast(FloatType)
    ).rdd.map { case Row(features: Vector, label: Float, baseMargin: Float, weight: Float) =>
      val (indices, values) = features match {
        case v: SparseVector => (v.indices, v.values.map(_.toFloat))
        case v: DenseVector => (null, v.values.map(_.toFloat))
      }
      XGBLabeledPoint(label, indices, values, baseMargin = baseMargin, weight = weight)
    }
    transformSchema(dataset.schema, logging = true)
    val derivedXGBParamMap = MLlib2XGBoostParams
    // All non-null param maps in XGBoostClassifier are in derivedXGBParamMap.
    val (_booster, _metrics) = WowXGBoost.trainDistributed(instances, derivedXGBParamMap,
      $(numRound), $(numWorkers), $(customObj), $(customEval), $(useExternalMemory),
      $(missing))
    val model = new XGBoostClassificationModel(uid, _numClasses, _booster)
    val summary = XGBoostTrainingSummary(_metrics)
    model.setSummary(summary)
    model
  }
}
