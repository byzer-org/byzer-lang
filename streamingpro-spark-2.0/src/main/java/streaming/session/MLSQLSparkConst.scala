package streaming.session

import scala.util.matching.Regex

/**
  * Created by allwefantasy on 1/6/2018.
  */
object MLSQLSparkConst {
  val SPARK_PREFIX = "spark."
  val YARN_PREFIX = "yarn."
  val HADOOP_PRFIX = "hadoop."
  val SPARK_HADOOP_PREFIX = SPARK_PREFIX + HADOOP_PRFIX
  val DRIVER_PREFIX = "driver."

  val KEYTAB = SPARK_PREFIX + YARN_PREFIX + "keytab"
  val PRINCIPAL = SPARK_PREFIX + YARN_PREFIX + "principal"
  val DRIVER_BIND_ADDR = SPARK_PREFIX + DRIVER_PREFIX + "bindAddress"

  val HIVE_VAR_PREFIX: Regex = """set:hivevar:([^=]+)""".r
  val USE_DB: Regex = """use:([^=]+)""".r
  val QUEUE = SPARK_PREFIX + YARN_PREFIX + "queue"
  val DEPRECATED_QUEUE = "mapred.job.queue.name"

  val SPARK_VERSION = org.apache.spark.SPARK_VERSION
}
