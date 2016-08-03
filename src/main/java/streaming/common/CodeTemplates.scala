package streaming.common

import scala.io.Source

/**
 * 8/3/16 WilliamZhu(allwefantasy@gmail.com)
 */
object CodeTemplates {

  val spark_before_2_0_rest_json_source_string = Source.fromInputStream(
    CodeTemplates.getClass.getResourceAsStream("/source/rest_json_before_2.source")).getLines().mkString("\n")

  val spark_before_2_0_rest_json_source_clazz = Array(
    "classOf[org.apache.spark.sql.execution.datasources.rest.json.DefaultSource]",
    "classOf[org.apache.spark.sql.execution.datasources.rest.json.InferSchema]",
    "classOf[org.apache.spark.sql.execution.datasources.rest.json.InferSchema$$anonfun$11]"
  )


  val spark_after_2_0_rest_json_source_string = Source.fromInputStream(
    CodeTemplates.getClass.getResourceAsStream("/source/rest_json_after_2.source")).getLines().mkString("\n")

  val spark_after_2_0_rest_json_source_clazz = Array(
    "classOf[org.apache.spark.sql.execution.datasources.rest.json.DefaultSource]"
  )
}
