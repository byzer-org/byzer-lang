package tech.mlsql

/**
  * 2019-05-30 WilliamZhu(allwefantasy@gmail.com)
  */
object MLSQLEnvKey {
  val REALTIME_LOG_HOME = "REALTIME_LOG_HOME"

  def realTimeLogHome = {
    val item = System.getenv(REALTIME_LOG_HOME)
    if (item == null) "/tmp/__mlsql__/logs"
    else item
  }
}
