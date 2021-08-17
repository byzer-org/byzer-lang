package tech.mlsql.example.app

import streaming.core.StreamingApp

/**
 * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
 */
object LocalSparkServiceApp {
  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[*]",
      "-streaming.name", "god",
      "-streaming.rest", "true",
      "-streaming.thrift", "false",
      "-streaming.platform", "spark",
      "-spark.mlsql.enable.runtime.directQuery.auth", "true",
      //      "-streaming.ps.cluster.enable","false",
      "-streaming.enableHiveSupport", "false",
      "-spark.mlsql.datalake.overwrite.hive", "true",
      "-spark.mlsql.auth.access_token", "mlsql",
      "-spark.hadoop.parquet.strings.signed-min-max.enabled", "true",
      //"-spark.mlsql.enable.max.result.limit", "true",
      //"-spark.mlsql.restful.api.max.result.size", "7",
      //      "-spark.mlsql.enable.datasource.rewrite", "true",
      //      "-spark.mlsql.datasource.rewrite.implClass", "streaming.core.datasource.impl.TestRewrite",
      //"-streaming.job.file.path", "classpath:///test/init.json",
      "-streaming.spark.service", "true",
      "-streaming.job.cancel", "true",
      "-streaming.datalake.path", "/Users/allwefantasy/data/mlsql/datalake/",

      //      "-streaming.plugin.clzznames","tech.mlsql.plugins.ds.MLSQLExcelApp",

      // scheduler
      "-streaming.workAs.schedulerService", "false",
      "-streaming.workAs.schedulerService.consoleUrl", "http://127.0.0.1:19002",
      "-streaming.workAs.schedulerService.consoleToken", "mlsql",


      //      "-spark.sql.hive.thriftServer.singleSession", "true",
      "-streaming.rest.intercept.clzz", "streaming.rest.ExampleRestInterceptor",
      //      "-streaming.deploy.rest.api", "true",
      "-spark.driver.maxResultSize", "2g",
      "-spark.serializer", "org.apache.spark.serializer.KryoSerializer",
      //      "-spark.sql.codegen.wholeStage", "true",
      "-spark.ui.allowFramingFrom", "*",
      "-spark.kryoserializer.buffer.max", "2000m",
      "-streaming.driver.port", "19003"
      //      "-spark.files.maxPartitionBytes", "10485760"

      //meta store
      //      "-streaming.metastore.db.type", "mysql",
      //      "-streaming.metastore.db.name", "app_runtime_full",
      //      "-streaming.metastore.db.config.path", "./__mlsql__/db.yml"

      //      "-spark.sql.shuffle.partitions", "1",
      //      "-spark.hadoop.mapreduce.job.run-local", "true"

      //"-streaming.sql.out.path","file:///tmp/test/pdate=20160809"

      //"-streaming.jobs","idf-compute"
      //"-streaming.sql.source.path","hdfs://m2:8020/data/raw/live-hls-formated/20160725/19/cdn148-16-52_2016072519.1469444764341"
      //"-streaming.driver.port", "9005"
      //"-streaming.zk.servers", "127.0.0.1",
      //"-streaming.zk.conf_root_dir", "/streamingpro/jack"
    ))
  }
}
