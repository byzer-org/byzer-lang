package tech.mlsql

/**
  * 2019-05-30 WilliamZhu(allwefantasy@gmail.com)
  */
object MLSQLEnvKey {
  val REALTIME_LOG_HOME = "REALTIME_LOG_HOME"

  // when this key is set true in context 
  // then we will skip size limit in RestController and to avoid executing in executor
  val CONTEXT_SYSTEM_TABLE = "context_system_table"

  def realTimeLogHome = {
    val item = System.getenv(REALTIME_LOG_HOME)
    if (item == null) "/tmp/__mlsql__/logs"
    else item
  }
}
