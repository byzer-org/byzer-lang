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

import org.apache.spark.ml.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.feature
import org.apache.spark.sql.{DataFrame, MLSQLUtils, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}


/**
  * Created by allwefantasy on 4/2/2018.
  */
class SQLStandardScaler extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val rfc = new StandardScaler()
    configureModel(rfc, params)
    val model = rfc.fit(df)
    model.write.overwrite().save(path)
    emptyDataFrame()(df)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val model = StandardScalerModel.load(path)
    model
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[StandardScalerModel])
    val std = getModelField(model.value, "std").asInstanceOf[Vector]
    val mean = getModelField(model.value, "mean").asInstanceOf[Vector]
    val withStd = model.value.getWithStd
    val withMean = model.value.getWithMean
    val scaler = new org.apache.spark.mllib.feature.StandardScalerModel(OldVectors.fromML(std), OldVectors.fromML(mean), withStd, withMean)
    val f: Vector => Vector = v => scaler.transform(OldVectors.fromML(v)).asML
    MLSQLUtils.createUserDefinedFunction(f, VectorType, Some(Seq(VectorType)))
  }
}
