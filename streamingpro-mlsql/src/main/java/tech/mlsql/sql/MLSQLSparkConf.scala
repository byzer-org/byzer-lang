package tech.mlsql.sql

import streaming.dsl.ScriptSQLExec

/**
  * 2019-05-19 WilliamZhu(allwefantasy@gmail.com)
  */
object MLSQLSparkConf {
  private def _conf() = {
    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.sparkSession
      .sparkContext
      .getConf
  }

  def runtimeDirectQueryAuth = {
    _conf().getBoolean("spark.mlsql.enable.runtime.directQuery.auth", false)
  }

}
