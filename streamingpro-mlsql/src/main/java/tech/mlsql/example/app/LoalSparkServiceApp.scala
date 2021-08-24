package tech.mlsql.example.app

import streaming.core.StreamingApp

/**
 * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
 */
object LocalSparkServiceApp {

  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[*]",
      "-streaming.name", "Mlsql-desktop",
      "-streaming.rest", "true",
      "-streaming.thrift", "false",
      "-streaming.platform", "spark",
      "-streaming.spark.service", "true",
      "-streaming.job.cancel", "true",
      "-streaming.datalake.path", "./data/",
      "-streaming.driver.port", "9003"
    ) ++ args )
  }
}
