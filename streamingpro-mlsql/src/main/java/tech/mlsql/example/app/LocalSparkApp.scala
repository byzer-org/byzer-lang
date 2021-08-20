package tech.mlsql.example.app

import streaming.core.StreamingApp

/**
 * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
 */
object LocalSparkApp {
  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[*]",
      "-streaming.name", "god",
      "-streaming.rest", "false",
      "-streaming.thrift", "false",
      "-streaming.platform", "spark",
      "-streaming.enableHiveSupport", "false",
      "-spark.mlsql.auth.access_token", "mlsql",
      "-streaming.spark.service", "false",
      "-streaming.job.cancel", "true",
      "-streaming.driver.port", "9003",


      "-streaming.runtime_hooks", "tech.mlsql.runtime.SparkSubmitMLSQLScriptRuntimeLifecycle",
      "-streaming.mlsql.script.owner", "admin",
      "-streaming.mlsql.sctipt.jobName", "test.mlsql",
      "-streaming.mlsql.script.path", "/tmp/test.mlsql"

    ))
  }
}
