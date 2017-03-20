package streaming.core

/**
 * 5/11/16 WilliamZhu(allwefantasy@gmail.com)
 */
object LocalFlinkStreamingApp {
  /*
  mvn package -Ponline -Pcarbondata -Pbuild-distr -Phive-thrift-server -Pspark-1.6.1
   */
  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[2]",
      "-streaming.name", "god",
      "-streaming.rest", "false",
      "-streaming.platform", "flink_streaming",
      "-streaming.spark.service", "false"

    ))
  }
}
