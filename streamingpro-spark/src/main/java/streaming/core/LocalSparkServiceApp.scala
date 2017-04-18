package streaming.core

/**
  * 5/11/16 WilliamZhu(allwefantasy@gmail.com)
  */
object LocalSparkServiceApp {
  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[2]",
      "-streaming.name", "god",
      "-streaming.rest", "true",
      "-streaming.thrift", "false",
      "-streaming.platform", "spark",
      "-streaming.job.file.path", "classpath:///test/empty.json",
      "-streaming.enableHiveSupport", "true",
      "-streaming.spark.service", "true",
      "-streaming.enableCarbonDataSupport", "true",
      "-streaming.carbondata.store", "/data/carbon/store",
      "-streaming.carbondata.meta", "/data/carbon/meta",
      "-spark.sql.hive.thriftServer.singleSession","true",
      "-streaming.driver.port", "9004"
      //"-streaming.sql.out.path","file:///tmp/test/pdate=20160809"

      //"-streaming.jobs","idf-compute"
      //"-streaming.sql.source.path","hdfs://m2:8020/data/raw/live-hls-formated/20160725/19/cdn148-16-52_2016072519.1469444764341"
      //"-streaming.driver.port", "9005"
      //"-streaming.zk.servers", "127.0.0.1",
      //"-streaming.zk.conf_root_dir", "/streamingpro/jack"
    ))
  }
}
