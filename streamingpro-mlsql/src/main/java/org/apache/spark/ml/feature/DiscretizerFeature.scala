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

package org.apache.spark.ml.feature

import org.apache.commons.lang3.math.NumberUtils
import org.apache.spark.sql.SparkSession
import streaming.dsl.mmlib.algs.{DiscretizerParamsConstrant, DiscretizerTrainData}

/**
 * Created by dxy_why on 2018/5/29.
 */
object DiscretizerFeature {
  val BUCKETIZER_METHOD = "bucketizer"
  val QUANTILE_METHOD = "quantile"

  def parseParams(params: Map[String, String], splits: Array[Double]): DiscretizerTrainData = {
    val handleInvalid = params.getOrElse(DiscretizerParamsConstrant.HANDLE_INVALID, "keep") == Bucketizer.KEEP_INVALID
    val inputCol = params.getOrElse(DiscretizerParamsConstrant.INPUT_COLUMN, null)
    require(inputCol != null, "inputCol should be configured.")
    DiscretizerTrainData(inputCol, splits, handleInvalid, params)
  }

  def getSplits(params: Map[String, String]): Array[Double] = {
    params.getOrElse("splitArray", "-inf,0.0,1.0,inf")
      .split(",").map(f => {
      f match {
        case "-inf" => Double.NegativeInfinity
        case "inf" => Double.PositiveInfinity
        case _ => NumberUtils.toDouble(f)
      }
    })
  }

  def getSplits(arrayString: String): Array[Double] = {
    arrayString.split(",").map(f => {
      f match {
        case "-inf" => Double.NegativeInfinity
        case "inf" => Double.PositiveInfinity
        case _ => NumberUtils.toDouble(f)
      }
    })
  }

  def binarySearchForBuckets(splits: Array[Double], feature: Double, keepInvalid: Boolean): Double = {
    Bucketizer.binarySearchForBuckets(splits, feature, keepInvalid)
  }

  def strColIndexSearch(labelArray: Array[String], feature: String): Double = {
    val idx = labelArray.indexOf(feature)
    idx.toDouble
  }

  def getDiscretizerPredictFun(spark: SparkSession, metas: Array[DiscretizerTrainData]): Seq[Double] => Seq[Double] = {

    val metasbc = spark.sparkContext.broadcast(metas)
    val transformer: Seq[Double] => Seq[Double] = features => {
      features.zipWithIndex.map {
        case (feature, index) =>
          val meta = metasbc.value(index)
          binarySearchForBuckets(meta.splits, feature, meta.handleInvalid)
      }
    }
    transformer

  }

}