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

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{ChiSqSelector, DCT, PCA, PolynomialExpansion}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 26/7/2018.
  */
class SQLReduceFeaturesInPlace extends SQLAlg with MllibFunctions with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val featureReduceType = params.getOrElse("featureReduceType", "pca")

    val model = featureReduceType.toLowerCase() match {
      case "pca" =>
        trainModels(df, path, params, () => {
          new PCA()
        })
      case "pe" =>
        trainModels(df, path, params, () => {
          new PolynomialExpansion()
        })
      case "dct" =>
        trainModels(df, path, params, () => {
          new DCT()
        })
      case "chisq" =>
        trainModels(df, path, params, () => {
          new ChiSqSelector()
        })
    }

    model.asInstanceOf[Transformer].transform(df).write.mode(SaveMode.Overwrite).parquet(path + "/data")
    emptyDataFrame()(df)

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}
