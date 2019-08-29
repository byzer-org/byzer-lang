package streaming.test.datasource

import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.log.Logging

/**
  * 2019-01-10 WilliamZhu(allwefantasy@gmail.com)
  */
class RedisSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll with Logging {
  "load solr" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>

      implicit val spark = runtime.sparkSession

      var sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |
           |connect redis
           |where host=""
           |and port="6379"
           |and dbNum="0"
           |as redis_test;
           |
           |select "a" as id, "b" as ck
           |as tmp_test1;
           |
           |save overwrite tmp_test1
           |as redis.`redis_test.tmp_test1_key`
           |options insertType="listInsertAsString";
         """.stripMargin, sq)

      val (a, b) = server.exec("redis", "redis-cli get tmp_test1_key")
      assume((a + b) == "a")
    }
  }

  val server = new streaming.test.servers.RedisServer("5.0.3")

  override protected def beforeAll(): Unit = {
    server.startServer
  }

  override protected def afterAll(): Unit = {
    server.stopServer
  }
}
