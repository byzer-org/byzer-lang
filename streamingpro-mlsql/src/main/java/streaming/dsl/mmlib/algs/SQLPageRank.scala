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

import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, MLSQLUtils, Row, SaveMode, SparkSession}
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLPageRank extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val vertexCol = params.getOrElse("vertexCol", "vertextIds").toString
    val edgeSrcCol = params.getOrElse("edgeSrcCol", "edgeSrc").toString
    val edgeDstCol = params.getOrElse("edgeDstCol", "edgeDst").toString

    val edges: RDD[Edge[Any]] = df.rdd.map { f =>
      Edge(f.getAs[Long](edgeSrcCol), f.getAs[Long](edgeDstCol))
    }

    var vertex: RDD[(VertexId, Any)] = null
    if (params.contains("vertexCol")) {
      vertex = df.rdd.map { f =>
        (f.getAs[Long](vertexCol), "")
      }
    } else {
      vertex = edges.flatMap(f => List(f.srcId, f.dstId)).distinct().map { f =>
        (f, "")
      }
    }

    val graph = Graph(vertex, edges, ())
    val tol = params.getOrElse("tol", "0.001").toDouble
    val resetProb = params.getOrElse("resetProb", "0.15").toDouble
    val pr = PageRank.runUntilConvergence(graph, tol, resetProb)
    val verticesDf = df.sparkSession.createDataFrame(pr.vertices.map(f => Row(f._1, f._2)),
      StructType(Seq(StructField("vertextId", LongType), StructField("pagerank", DoubleType))))

    val edgesDf = df.sparkSession.createDataFrame(pr.edges.map(f => Row(f.srcId, f.dstId, f.attr)),
      StructType(Seq(StructField("srcId", LongType), StructField("dstId", LongType), StructField("weight", DoubleType))))

    verticesDf.write.mode(SaveMode.Overwrite).parquet(path + "/vertices")
    edgesDf.write.mode(SaveMode.Overwrite).parquet(path + "/edges")
    emptyDataFrame()(df)

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    import sparkSession._
    val verticesDf = sparkSession.read.parquet(path + "/vertices")
    verticesDf.collect().map(f => (f.getLong(0), f.getDouble(1))).toMap
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[Map[Long, Double]])

    val f = (vertexId: Long) => {
      model.value(vertexId)
    }
    MLSQLUtils.createUserDefinedFunction(f, DoubleType, Some(Seq(LongType)))
  }
}
