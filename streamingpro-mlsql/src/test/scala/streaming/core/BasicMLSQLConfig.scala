package streaming.core

/**
  * Created by allwefantasy on 6/5/2018.
  */
trait BasicMLSQLConfig {
  val batchParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "false",
    "-streaming.spark.service", "false",
    "-streaming.udf.clzznames", "streaming.crawler.udf.Functions,streaming.dsl.mmlib.algs.processing.UDFFunctions",
    "-streaming.unittest", "true"
  )

  val batchParamsWithPort = Array(
    "-streaming.master", "local[*]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "true",
    "-streaming.driver.port", "9003",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "true",
    "-streaming.spark.service", "false",
    "-streaming.udf.clzznames", "streaming.crawler.udf.Functions,streaming.dsl.mmlib.algs.processing.UDFFunctions",
    "-streaming.unittest", "true"
  )

  val batchParamsWithCarbondata = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "true",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true",
    "-streaming.enableCarbonDataSupport", "true",
    "-streaming.udf.clzznames", "streaming.crawler.udf.Functions,streaming.dsl.mmlib.algs.processing.UDFFunctions",
    "-streaming.carbondata.store", "/tmp/carbondata/store",
    "-streaming.carbondata.meta", "/tmp/carbondata/meta"
  )

  val mlsqlParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "mlsql",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true"

  )
}
