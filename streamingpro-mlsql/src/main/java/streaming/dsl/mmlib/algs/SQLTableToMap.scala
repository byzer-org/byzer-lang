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

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, MLSQLUtils, Row, SaveMode, SparkSession, functions => F}
import org.apache.spark.sql.types.{ArrayType, StringType}
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 8/2/2018.
  */
class SQLTableToMap extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val keyField = params.getOrElse("keyField", "key")
    val valueField = params.getOrElse("keyField", "value")

    val mapResult = df.select(F.col(keyField).as("key"), F.col(valueField).as("value"))
    mapResult.write.mode(SaveMode.Overwrite).parquet(path)
    emptyDataFrame()(df)

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    sparkSession.read.parquet(path).collect().map(f => (f.get(0).toString, f.get(1).toString)).toMap
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[Map[String, String]])
    val f = (name: String) => {
      model.value(name)
    }
    MLSQLUtils.createUserDefinedFunction(f, StringType, Some(Seq(StringType)))
  }
}
