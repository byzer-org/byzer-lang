package streaming.core

/**
 * 5/11/16 WilliamZhu(allwefantasy@gmail.com)
 */
object LocalSparkApp {
  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[2]",
      "-streaming.name", "god",
      "-streaming.rest", "true",
      "-streaming.platform", "spark",
      "-streaming.spark.service", "true",
      "-streaming.driver.port", "9004",
      "-streaming.zk.servers", "127.0.0.1",
      "-streaming.zk.conf_root_dir", "/streamingpro/jack"
    ))
  }
}
