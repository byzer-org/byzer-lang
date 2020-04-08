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

import org.apache.spark.sql.{DataFrame, MLSQLUtils, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 8/2/2018.
  */
class SQLDicOrTableToArray extends SQLAlg with Functions {

  def internal_train(df: DataFrame, params: Map[String, String]) = {
    val session = df.sparkSession
    var result = Array[Row]()
    if (params.contains("dic.paths")) {
      result ++= params("dic.names").split(",").zip(params("dic.paths").split(",")).map { f =>
        val wordsList = session.sparkContext.textFile(f._2).collect()
        (f._1, wordsList)
      }.map(f => Row.fromSeq(Seq(f._1, f._2)))

    }

    if (params.contains("table.paths")) {
      result ++= params("table.names").split(",").zip(params("table.paths").split(",")).map { f =>
        val wordsList = session.table(f._2).collect().map(f => f.get(0).toString)
        (f._1, wordsList)
      }.map(f => Row.fromSeq(Seq(f._1, f._2)))
    }

    val model = session.createDataFrame(
      session.sparkContext.parallelize(result),
      StructType(Seq(StructField("name", StringType), StructField("tokens", ArrayType(StringType)))))
    //model is also is a table
    model
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val model = internal_train(df, params)
    model.write.mode(SaveMode.Overwrite).parquet(path)
    emptyDataFrame()(df)

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    sparkSession.read.parquet(path).collect().map(f => (f.getString(0), f.getSeq(1))).toMap
  }

  def internal_predict(sparkSession: SparkSession, _model: Any, name: String) = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[Map[String, Seq[String]]])
    val f = (name: String) => {
      model.value(name)
    }
    Map(name -> f)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val ip = internal_predict(sparkSession, _model, name)
    MLSQLUtils.createUserDefinedFunction(ip(name), ArrayType(StringType), Some(Seq(StringType)))
  }
}
