package streaming.core

/**
  * Created by allwefantasy on 27/3/2017.
  */
object LocalStreamingApp {
  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[2]",
      "-streaming.duration", "10",
      "-spark.sql.shuffle.partitions","1",
      "-streaming.name", "god",
      "-streaming.rest", "true"
      ,"-streaming.driver.port","9003",
      "-streaming.platform", "spark_structured_streaming",
      "-streaming.job.file.path", "classpath:///test/ss-test.json"
    ))
  }
}
