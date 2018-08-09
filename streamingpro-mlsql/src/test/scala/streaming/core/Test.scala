package streaming.core

import java.net.URL

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}
import streaming.common.{PunctuationUtils, UnicodeUtils}

import scala.collection.mutable

/**
  * Created by allwefantasy on 28/3/2017.
  */
object Test {
  def main(args: Array[String]): Unit = {
    // Create an RDD for the vertices
    val sparkSession = SparkSession.builder().master("local[*]").appName("wow").getOrCreate()
    val sc = sparkSession.sparkContext
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[Double]] =
    sc.parallelize(Array(Edge(3L, 7L, 0.9), Edge(5L, 3L, 0.9),
      Edge(2L, 5L, 0.1), Edge(5L, 7L, 0.8),
      Edge(4L, 0L, 0.9), Edge(5L, 0L, 0.2)))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))

    val validGraph = graph.subgraph(epred = et => {
      et.attr > 0.7
    }).connectedComponents()

    val wow = validGraph.vertices.map(f => VeterxAndGroup(f._1, f._2)).groupBy(f => f.group).sortBy(f => -f._2.size).take(1).head
    wow._2.foreach(f => println(f.vertexId))

  }
}

case class VeterxAndGroup(vertexId: VertexId, group: VertexId)
