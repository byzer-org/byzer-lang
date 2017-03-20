package streaming.common

import org.apache.spark.sql.functions2
import org.apache.spark.util.ScalaSourceCodeCompiler

import scala.io.Source

/**
 * 8/3/16 WilliamZhu(allwefantasy@gmail.com)
 */
object CodeTemplates {

  val spark_before_2_0_rest_json_source_string = readFile("/source/rest_json_before_2.source")

  val spark_before_2_0_rest_json_source_clazz = Array(
    "classOf[org.apache.spark.sql.execution.datasources.rest.json.DefaultSource]",
    "classOf[org.apache.spark.sql.execution.datasources.rest.json.InferSchema]",
    "classOf[org.apache.spark.sql.execution.datasources.rest.json.InferSchema$$anonfun$11]"
  )


  val spark_after_2_0_rest_json_source_string = readFile("/source/rest_json_after_2.source")

  val spark_after_2_0_rest_json_source_clazz = Array(
    "classOf[org.apache.spark.sql.execution.datasources.rest.json.DefaultSource]"
  )


  //vectors
  val vectorize_after_2_0_str = readFile("/source/vectorize_after_2_0_str.source")
  val vectorize_before_2_0_str = readFile("/source/vectorize_before_2_0_str.source")

  def readFile(path: String) = {
    Source.fromInputStream(
      CodeTemplates.getClass.getResourceAsStream(path)).getLines().mkString("\n")
  }


}

object CodeTemplateFunctions {
  def vectorize(vectorSize: Int) = {
    if (org.apache.spark.SPARK_VERSION.startsWith("2"))
      ScalaSourceCodeCompiler.
        compileCode(CodeTemplates.vectorize_after_2_0_str.replace("vectorSize", vectorSize.toString))
    else
      ScalaSourceCodeCompiler.compileCode(CodeTemplates.
        vectorize_before_2_0_str.replace("vectorSize", vectorSize.toString))
  }

  def vectorizeByReflect(vectorSize: Int) = {

    val clzzName =
      if (org.apache.spark.SPARK_VERSION.startsWith("2")) {
        "org.apache.spark.ml.linalg.Vectors"
      } else {
        "org.apache.spark.mllib.linalg.Vectors"
      }

    val reslutClzzName = if (org.apache.spark.SPARK_VERSION.startsWith("2")) {
      "org.apache.spark.ml.linalg.Vector"
    } else {
      "org.apache.spark.mllib.linalg.Vector"
    }
    def dense(v: Array[Double]) = {
      Class.forName(clzzName).getMethod("dense", classOf[Array[Double]]).invoke(null, v)
    }

    def sparse(vectorSize: Int, v: Array[(Int, Double)]) = {
      val method = Class.forName(clzzName).getMethod("sparse", classOf[Int], classOf[Seq[(Int, Double)]])
      val vs: Integer = vectorSize
      method.invoke(null, vs, v.toSeq)
    }

    val t = functions2.udf(reslutClzzName, (features: String) => {
      if (!features.contains(":")) {
        val v = features.split(",|\\s+").map(_.toDouble)
        dense(v)
      } else {
        val v = features.split(",|\\s+").map(_.split(":")).map(f => (f(0).toInt, f(1).toDouble))
        sparse(vectorSize, v)
      }
    })

    t
  }

}


