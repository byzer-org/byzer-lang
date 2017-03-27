package streaming.core

/**
  * Created by allwefantasy on 27/3/2017.
  */
class LocalStreamingApp {
  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[2]",
      "-streaming.duration", "10",
      "-spark.sql.shuffle.partitions","1",
      "-streaming.name", "god",
      "-streaming.rest", "false"
      ,"-streaming.driver.port","9902",
      "-streaming.platform", "spark_streaming",
      "-streaming.sql.out.jack.user","root",
      "-streaming.sql.out.jack.password","csdn.net",
      "-streaming.sql.out.jack.url","jdbc:mysql://127.0.0.1/alarm_test?characterEncoding=utf8"
    ))
  }
}
