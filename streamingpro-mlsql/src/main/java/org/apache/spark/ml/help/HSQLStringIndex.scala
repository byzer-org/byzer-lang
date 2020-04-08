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

package org.apache.spark.ml.help

import org.apache.spark.{MLSQLSparkConst, SparkException}
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.sql.{MLSQLUtils, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.util.collection.OpenHashMap

/**
  * Created by allwefantasy on 15/1/2018.
  */
object HSQLStringIndex {
  def predict(sparkSession: SparkSession, _model: Any, name: String) = {

    val res = internal_predict(sparkSession, _model, name)

    sparkSession.udf.register(name + "_array", res(name + "_array").asInstanceOf[Seq[String] => Seq[Int]])
    sparkSession.udf.register(name + "_r", res(name + "_r").asInstanceOf[Double => String])
    sparkSession.udf.register(name + "_rarray", res(name + "_rarray").asInstanceOf[Seq[Double] => Seq[String]])

    MLSQLUtils.createUserDefinedFunction(res(name), IntegerType, Some(Seq(StringType)))
  }

  def wordToIndex(sparkSession: SparkSession, _model: Any) = {
    val model = _model.asInstanceOf[StringIndexerModel]
    val labelToIndexField = model.getClass.getDeclaredField("org$apache$spark$ml$feature$StringIndexerModel$$labelToIndex")
    labelToIndexField.setAccessible(true)
    labelToIndexField.get(model).asInstanceOf[OpenHashMap[String, Double]]
  }

  def internal_predict(sparkSession: SparkSession, _model: Any, name: String) = {

    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[StringIndexerModel])

    val f = (label: String) => {

      val labelToIndexField = model.value.getClass.getDeclaredField("org$apache$spark$ml$feature$StringIndexerModel$$labelToIndex")
      labelToIndexField.setAccessible(true)
      val labelToIndex = labelToIndexField.get(model.value).asInstanceOf[OpenHashMap[String, Double]]

      if (label == null) {
        if (model.value.getHandleInvalid == "keep" || model.value.getHandleInvalid == "skip") {
          -1
        } else {
          throw new SparkException("StringIndexer encountered NULL value. To handle or skip " +
            "NULLS, try setting StringIndexer.handleInvalid.")
        }
      } else {
        if (labelToIndex.contains(label)) {
          labelToIndex(label).toInt
        } else {
          -1
        }
        //        } else if (model.value.getHandleInvalid == "keep" || model.value.getHandleInvalid == "skip") {
        //          -1
        //        } else {
        //          throw new SparkException(s"Unseen label: $label.  To handle unseen labels, " +
        //            s"set Param handleInvalid to keep.")
        //        }
      }
    }

    val f2 = (labels: Seq[String]) => {
      if (model.value.getHandleInvalid == "keep")
        labels.map(label => f(label)).toArray
      else labels.map(label => f(label)).filterNot(f => f == -1).toArray
    }

    val f_r = (index: Double) => {
      if (model.value.labels.length <= index.toInt || index.toInt < 0) {
        "__unknow__"
      }
      else
        model.value.labels(index.toInt)
    }

    val f_rarray = (indexs: Seq[Double]) => {
      indexs.map(index => f_r(index)).filterNot(f => f == "__unknow__").toArray
    }
    Map((name + "_array") -> f2,
      (name + "_r") -> f_r,
      (name + "_rarray") -> f_rarray,
      name -> f
    )
  }
}
