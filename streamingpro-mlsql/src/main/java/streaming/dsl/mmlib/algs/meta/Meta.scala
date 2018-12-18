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

package streaming.dsl.mmlib.algs.meta

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType
import streaming.dsl.mmlib.algs.DiscretizerTrainData

/**
  * Created by allwefantasy on 22/5/2018.
  */
case class TFIDFMeta(trainParams: Map[String, String], wordIndex: Map[String, Double], tfidfFunc: (Seq[Int] => org.apache.spark.ml.linalg.Vector))

case class Word2VecMeta(trainParams: Map[String, String], wordIndex: Map[String, Double], predictFunc: ((Seq[String]) => Seq[Seq[Double]]))

case class Word2IndexMeta(trainParams: Map[String, String], wordIndex: Map[String, Double])

case class ScaleMeta(trainParams: Map[String, String], removeOutlierValueFunc: (Double, String) => Double, scaleFunc: Vector => Vector)

case class OutlierValueMeta(fieldName: String, lowerRange: Double, upperRange: Double, quantile: Double)

case class MinMaxValueMeta(fieldName: String, min: Double, max: Double)

case class StandardScalerValueMeta(fieldName: String, mean: Array[Double], std: Array[Double])

//case class DiscretizerMeta(params: Array[DiscretizerTrainData], discretizerFunc: Seq[Double] => Seq[Double])
case class DiscretizerMeta(params: Array[DiscretizerTrainData], discretizerFunc: Seq[Double] => Seq[Double])

case class Word2ArrayMeta(trainParams: Map[String, String], words: Set[String])

case class MapValuesMeta(
    inputCol: String,
    outputCol: String,
    mapMissingTo: String)
