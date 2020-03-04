package tech.mlsql

/**
 * 2019-05-30 WilliamZhu(allwefantasy@gmail.com)
 */
object MLSQLEnvKey {
  val REALTIME_LOG_HOME = "REALTIME_LOG_HOME"

  // when this key is set true in context 
  // then we will skip size limit in RestController and to avoid executing in executor
  val CONTEXT_SYSTEM_TABLE = "context_system_table"

  val CONTEXT_STATEMENT_NUM = "context_statement_num"

  val REQUEST_CONTEXT_ENABLE_SPARK_LOG = "enableSparkLog"

  val CONTEXT_KAFKA_SCHEMA = "context_kafka_schema"

  def realTimeLogHome = {
    "./logs"
  }
}
