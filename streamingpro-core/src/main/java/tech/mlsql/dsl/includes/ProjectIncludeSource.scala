package tech.mlsql.dsl.includes

import org.apache.spark.sql.SparkSession
import streaming.dsl.IncludeSource
import tech.mlsql.common.utils.log.Logging

import scala.io.Source

/**
 * 6/5/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class ProjectIncludeSource extends IncludeSource with Logging {
  override def fetchSource(sparkSession: SparkSession, path: String, options: Map[String, String]): String = {
    Source.fromFile(path).getLines().mkString("\n")
  }

  override def skipPathPrefix: Boolean = true
}