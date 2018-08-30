package streaming.dsl

import org.apache.spark.sql.SparkSession

/**
  * Created by allwefantasy on 30/8/2018.
  */
trait IncludeSource {
  def fetchSource(sparkSession: SparkSession, path: String, options: Map[String, String]): String

  def skipPathPrefix: Boolean = false
}
