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

import org.apache.spark.ml.BaseAlgorithmTransformer
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.DataFrame

/**
 * 7/28/16 WilliamZhu(allwefantasy@gmail.com)
 */
class LogicRegressionTransformer(path: String,parameters:Map[String,String])extends BaseAlgorithmTransformer {

  val model = LogisticRegressionModel.load(path)

  def transform(df: DataFrame): DataFrame = {
    model.transform(df)
  }

}
