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

import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, MLSQLUtils, Row, SaveMode, SparkSession}
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 22/1/2018.
  */
class SQLRowMatrix extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val rdd = df.rdd.map { f =>
      val v = f.getAs(params.getOrElse("inputCol", "features").toString).asInstanceOf[Vector]
      val label = f.getAs(params.getOrElse("labelCol", "label").toString).asInstanceOf[Long]
      IndexedRow(label, OldVectors.fromML(v))
    }

    val randRowMat = new IndexedRowMatrix(rdd).toBlockMatrix().transpose.toCoordinateMatrix().toRowMatrix()
    var threshhold = 0.0
    if (params.contains("threshhold")) {
      threshhold = params.get("threshhold").map(f => f.toDouble).head
    }
    val csl = randRowMat.columnSimilarities(threshhold)

    val newdf = df.sparkSession.createDataFrame(csl.entries.map(f => Row(f.i, f.j, f.value)),
      StructType(Seq(StructField("i", LongType), StructField("j", LongType), StructField("v", DoubleType))))
    newdf.write.mode(SaveMode.Overwrite).parquet(path)
    emptyDataFrame()(df)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val entries = sparkSession.read.parquet(path)
    entries.rdd.map { f =>
      (f.getLong(0), (f.getLong(1), f.getDouble(2)))
    }.groupByKey().map(f => (f._1, f._2.toMap)).collect().toMap
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[Map[Long, Map[Long, Double]]])

    val f = (i: Long, threshhold: Double) => {
      model.value(i).filter(f => f._2 > threshhold)
    }
    MLSQLUtils.createUserDefinedFunction(f, MapType(LongType, DoubleType), Some(Seq(LongType, DoubleType)))
  }
}
