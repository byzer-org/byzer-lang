package streaming.core

/**
 * 5/11/16 WilliamZhu(allwefantasy@gmail.com)
 */
object LocalSparkApp {
  /*
  mvn package -Ponline -Pcarbondata -Pbuild-distr -Phive-thrift-server -Pspark-1.6.1
   */
  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[2]",
      "-streaming.name", "god",
      "-streaming.rest", "false",
      "-streaming.platform", "spark",
      "-streaming.spark.service", "false",
      "-streaming.enableHiveSupport", "true",
      "-streaming.enableCarbonDataSupport", "true",
      "-streaming.carbondata.store", "/tmp/carbondata/store",
      "-streaming.carbondata.meta", "/tmp/carbondata/meta"
      //"-streaming.sql.out.path","file:///tmp/test/pdate=20160809"

      //"-streaming.jobs","idf-compute"
      //"-streaming.sql.source.path","hdfs://m2:8020/data/raw/live-hls-formated/20160725/19/cdn148-16-52_2016072519.1469444764341"
      //"-streaming.driver.port", "9005"
      //"-streaming.zk.servers", "127.0.0.1",
      //"-streaming.zk.conf_root_dir", "/streamingpro/jack"
    ))
  }
}
