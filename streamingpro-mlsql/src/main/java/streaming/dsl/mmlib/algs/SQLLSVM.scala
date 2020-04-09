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

import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier, LinearSVC, LinearSVCModel}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DoubleType
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.sql.{SparkSession, _}

/**
  * Created by allwefantasy on 15/1/2018.
  */
class SQLLSVM extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val bayes = new LinearSVC()
    configureModel(bayes, params)
    val model = bayes.fit(df)
    model.write.overwrite().save(path)
    emptyDataFrame()(df)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val model = LinearSVCModel.load(path)
    model
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[LinearSVCModel])

    val f = (vec: Vector) => {
      val predictRaw = model.value.getClass.getMethod("predictRaw", classOf[Vector]).invoke(model.value, vec).asInstanceOf[Vector]
      val raw2probability = model.value.getClass.getMethod("raw2prediction", classOf[Vector]).invoke(model.value, predictRaw).asInstanceOf[Double]
      //model.getClass.getMethod("probability2prediction", classOf[Vector]).invoke(model, raw2probability).asInstanceOf[Vector]
      raw2probability

    }
    MLSQLUtils.createUserDefinedFunction(f, DoubleType, Some(Seq(VectorType)))
  }
}
