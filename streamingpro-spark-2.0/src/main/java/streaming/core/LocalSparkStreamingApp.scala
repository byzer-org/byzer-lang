package streaming.core

/**
  * Created by allwefantasy on 27/3/2017.
  */
object LocalSparkStreamingApp {
  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[2]",
      "-streaming.duration", "10",
      "-spark.sql.shuffle.partitions", "1",
      "-streaming.name", "god",
      "-streaming.rest", "false",
      "-streaming.driver.port", "9003",
      "-streaming.platform", "spark_streaming",
      "-streaming.job.file.path", "classpath:///test/spark-streaming-test.json"
    ))
  }
}
