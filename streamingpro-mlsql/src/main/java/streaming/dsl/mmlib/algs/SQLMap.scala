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

import net.sf.json.JSONObject
import org.apache.spark.sql.{DataFrame, MLSQLUtils, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StringType
import streaming.dsl.mmlib.SQLAlg

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 16/8/2018.
  */
class SQLMap extends SQLAlg with MllibFunctions with Functions {

  override def skipPathPrefix: Boolean = true
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    emptyDataFrame()(df)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val res = JSONObject.fromObject(sparkSession.table(path).toJSON.head()).map(f => (f._1.toString(), f._2.toString())).toMap
    res
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val res = _model.asInstanceOf[Map[String, String]]
    val f = (a: String) => {
      res(a)
    }
    MLSQLUtils.createUserDefinedFunction(f, StringType, Some(Seq(StringType)))
  }
}
