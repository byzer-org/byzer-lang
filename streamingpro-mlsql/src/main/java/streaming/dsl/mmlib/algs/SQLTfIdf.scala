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

import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, IntTF}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType}
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.sql.{SparkSession, _}


/**
  * Created by allwefantasy on 17/1/2018.
  */
class SQLTfIdf extends SQLAlg with Functions {


  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val sparkSession = df.sparkSession
    val rfc = new IntTF()
    configureModel(rfc, params)
    rfc.setOutputCol("__SQLTfIdf__")
    val featurizedData = rfc.transform(df)
    
    val idf = new IDF()
    configureModel(idf, params)
    idf.setInputCol("__SQLTfIdf__")
    val idfModel = idf.fit(featurizedData)
    idfModel.write.overwrite().save(path)
    emptyDataFrame()(df)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val model = IDFModel.load(path)
    model
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val res = internal_predict(sparkSession, _model, name)
    MLSQLUtils.createUserDefinedFunction(res(name), VectorType, Some(Seq(ArrayType(IntegerType))))
  }

  def internal_predict(sparkSession: SparkSession, _model: Any, name: String) = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[IDFModel])
    val intTF = new org.apache.spark.mllib.feature.IntTF(model.value.idf.size).setBinary(false)
    val idf = (words: Seq[Int]) => {
      val idfModelField = model.value.getClass.getField("org$apache$spark$ml$feature$IDFModel$$idfModel")
      idfModelField.setAccessible(true)
      val idfModel = idfModelField.get(model.value).asInstanceOf[org.apache.spark.mllib.feature.IDFModel]
      val vec = intTF.transform(words)
      idfModel.transform(vec).asML
    }
    Map(name -> idf)
  }
}
