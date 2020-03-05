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

package streaming.dsl.load.batch

import org.apache.spark.MLSQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import tech.mlsql.sql.MLSQLSparkConf

import scala.collection.JavaConversions._

/**
  * 2018-11-30 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLConfExplain(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = false

  override def explain: DataFrame = {
    val items = MLSQLConf.entries.map(f => Row.fromSeq(Seq(f._2.key, f._2.defaultValueString, f._2.doc))).toSeq
    val items2 = MLSQLSparkConf.entries.map(f => Row.fromSeq(Seq(f._1, f._2.defaultValue.toString, f._2.doc))).toSeq


    val rows = sparkSession.sparkContext.parallelize(items ++ items2, 1)

    sparkSession.createDataFrame(rows,
      StructType(Seq(
        StructField(name = "name", dataType = StringType),
        StructField(name = "value", dataType = StringType),
        StructField(name = "doc", dataType = StringType)
      )))
  }
}

