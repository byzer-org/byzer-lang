package streaming.dsl.mmlib.algs.includes

import org.apache.spark.sql.SparkSession
import streaming.dsl.IncludeSource

/**
  * Created by allwefantasy on 27/8/2018.
  */
class HDFSIncludeSource extends IncludeSource {
  override def fetchSource(sparkSession: SparkSession, path: String, options: Map[String, String]): String = {
    sparkSession.sparkContext.textFile(path, 1).collect().mkString("\n")
  }

  override def skipPathPrefix: Boolean = false
}
