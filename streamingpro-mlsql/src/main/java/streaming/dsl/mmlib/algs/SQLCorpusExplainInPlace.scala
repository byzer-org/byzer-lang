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

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions => F}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 26/6/2018.
  */
class SQLCorpusExplainInPlace extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    require(params.contains("labelCol"), "labelCol is required")
    val labelCol = params.getOrElse("labelCol", "")

    val metaPath = MetaConst.getMetaPath(path)
    // keep params
    saveTraningParams(df.sparkSession, params, metaPath)

    val totalCount = df.count()
    val c = F.udf((labelCount: Int) => {
      totalCount.toDouble / labelCount
    })
    val c2 = F.udf((labelCount: Int) => {
      labelCount / totalCount.toDouble
    })

    val c3 = F.udf(() => {
      totalCount
    })
    val newDF = df.select(F.col(labelCol)).
      groupBy(labelCol).
      agg(F.count(labelCol).as("labelCount")).
      withColumn("weight", c(F.col("labelCount"))).
      withColumn("percent", c2(F.col("labelCount"))).
      withColumn("total", c3())
    newDF.write.mode(SaveMode.Overwrite).parquet(MetaConst.getDataPath(path))
    emptyDataFrame()(df)
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    //    val path = MetaConst.getMetaPath(_path)
    //    val df = spark.read.parquet(MetaConst.PARAMS_PATH(path, "params")).map(f => (f.getString(0), f.getString(1)))
    //    val trainParams = df.collect().toMap
    //    val labelCol = trainParams.getOrElse("labelCol", "")
    //
    //    val data = spark.read.parquet(MetaConst.getDataPath(_path))
    //    data.map { f =>
    //      Array(f.getAs[String](labelCol),)
    //    }
    throw new RuntimeException("register is not supported by this module")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException("register is not supported by this module")
  }
}
