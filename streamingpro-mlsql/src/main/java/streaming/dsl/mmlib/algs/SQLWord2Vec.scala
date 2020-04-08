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

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.{DataFrame, MLSQLUtils, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import streaming.dsl.mmlib.SQLAlg

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLWord2Vec extends SQLAlg with Functions {

  def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val w2v = new Word2Vec()
    configureModel(w2v, params)
    val model = w2v.fit(df)
    model.write.overwrite().save(path)
    emptyDataFrame()(df)
  }

  def load(sparkSession: SparkSession, path: String, params: Map[String, String]) = {
    val model = Word2VecModel.load(path)
    model.getVectors.collect().
      map(f => (f.getAs[String]("word"), f.getAs[DenseVector]("vector").toArray)).
      toMap
  }

  def internal_predict(sparkSession: SparkSession, _model: Any, name: String) = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[Map[String, Array[Double]]])

    val f = (co: String) => {
      model.value.get(co) match {
        case Some(vec) => vec.toSeq
        case None => Seq[Double]()
      }

    }

    val f2 = (co: Seq[String]) => {
      co.map(f(_)).filter(x => x.size > 0)
    }

    Map((name + "_array") -> f2, name -> f)
  }

  def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val res = internal_predict(sparkSession, _model, name)
    sparkSession.udf.register(name + "_array", res(name + "_array"))
    MLSQLUtils.createUserDefinedFunction(res(name), ArrayType(DoubleType), Some(Seq(StringType)))
  }
}
