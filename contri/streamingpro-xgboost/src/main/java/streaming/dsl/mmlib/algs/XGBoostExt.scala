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

package streaming.dsl.mmlib.algs

import ml.dmlc.xgboost4j.scala.spark.{TrackerConf, WowXGBoostClassifier, XGBoostClassificationModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by allwefantasy on 12/9/2018.
  */
class XGBoostExt {
  def WowXGBoostClassifier = {
    val xgboost = new WowXGBoostClassifier()
    xgboost.set(xgboost.trackerConf, new TrackerConf(0, "scala"))
    xgboost
  }

  def load(tempPath: String) = {
    XGBoostClassificationModel.load(tempPath)
  }

  def explainModel(sparkSession: SparkSession, models: ArrayBuffer[XGBoostClassificationModel]): DataFrame = {
    val rows = models.flatMap { model =>
      val modelParams = model.params.filter(param => model.isSet(param)).map { param =>
        val tmp = model.get(param).get
        val str = if (tmp == null) {
          "null"
        } else tmp.toString
        Seq(("fitParam.[group]." + param.name), str)
      }
      Seq() ++ modelParams
    }.map(Row.fromSeq(_))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows, 1),
      StructType(Seq(StructField("name", StringType), StructField("value", StringType))))
  }
}
