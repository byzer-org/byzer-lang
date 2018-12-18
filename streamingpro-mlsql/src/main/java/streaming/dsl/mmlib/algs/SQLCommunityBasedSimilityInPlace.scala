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

import org.apache.spark.graphx._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.meta.graphx.{GroupVeterxs, VeterxAndGroup}

/**
  * Created by allwefantasy on 9/8/2018.
  */
class SQLCommunityBasedSimilarityInPlace extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val rowNumCol = params.getOrElse("rowNum", "i")
    val columnNumCol = params.getOrElse("columnNum", "j")
    val edgeValueCol = params.getOrElse("edgeValue", "v")
    val minSimilarity = params.getOrElse("minSimilarity", "0.7").toDouble
    val minCommunitySize = params.getOrElse("minCommunitySize", "10").toInt
    val minCommunityPercent = params.getOrElse("minCommunityPercent", "0.1").toDouble

    val relationships = df.rdd.map(f => Edge(f.getAs[Long](rowNumCol), f.getAs[Long](columnNumCol), f.getAs[Double](edgeValueCol)))
    val graph = Graph.fromEdges(relationships, 0d)

    val vertexCount = Math.max(Math.round(graph.vertices.count() * minCommunityPercent), minCommunitySize)

    val validGraph = graph.subgraph(epred = et => {
      et.attr > minSimilarity
    }).connectedComponents()


    val rdd = validGraph.vertices.map(f => VeterxAndGroup(f._1, f._2)).groupBy(f => f.group).
      filter(f => f._2.size > vertexCount).
      map(f => GroupVeterxs(f._1, f._2.map(k => k.vertexId).toSeq))
    import df.sparkSession.implicits._
    df.sparkSession.createDataset(rdd).write.mode(SaveMode.Overwrite).parquet(path + "/data")
    emptyDataFrame()(df)

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    new UnsupportedOperationException("Register is not supported by SQLCommunityBasedSimilarityInPlace module")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }
}

