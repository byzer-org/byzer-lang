package streaming.core

/**
  * Created by allwefantasy on 30/3/2017.
  */
object LocalSparkServiceApp {
  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[*]",
      "-streaming.name", "god",
      "-streaming.rest", "true",
      "-streaming.thrift", "false",
      "-streaming.platform", "spark",
      "-streaming.job.file.path", "classpath:///test/empty.json",
      "-streaming.spark.service", "true",
      "-streaming.job.cancel", "true",
      "-streaming.ps.enable", "true",
      "-spark.sql.hive.thriftServer.singleSession", "true",
      "-streaming.rest.intercept.clzz", "streaming.rest.ExampleRestInterceptor",
      "-streaming.deploy.rest.api", "false",
      "-spark.driver.maxResultSize", "2g",
      "-spark.serializer", "org.apache.spark.serializer.KryoSerializer",
      "-spark.sql.codegen.wholeStage", "true",
      "-spark.kryoserializer.buffer.max", "2000m",
      "-streaming.udf.clzznames", "streaming.crawler.udf.Functions",
      "-streaming.driver.port", "9003",
      "-spark.sql.shuffle.partitions", "2"

      //"-streaming.sql.out.path","file:///tmp/test/pdate=20160809"

      //"-streaming.jobs","idf-compute"
      //"-streaming.sql.source.path","hdfs://m2:8020/data/raw/live-hls-formated/20160725/19/cdn148-16-52_2016072519.1469444764341"
      //"-streaming.driver.port", "9005"
      //"-streaming.zk.servers", "127.0.0.1",
      //"-streaming.zk.conf_root_dir", "/streamingpro/jack"
    ))
  }
}
