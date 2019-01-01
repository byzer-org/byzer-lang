package streaming.test.datasource

import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.log.Logging

/**
  * Created by pigeongeng on 2018/12/27.4:19 PM
  *
  */
class SolrSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll with Logging {
  "load solr" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>

      implicit val spark = runtime.sparkSession

      var sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |
           |select 1 as id, "this is mlsql_example" as title_s as mlsql_example_data;
           |
           |connect solr where `zkhost`="127.0.0.1:9983"
           |and `collection`="mlsql_example"
           |and `flatten_multivalued`="false"
           |as solr1
           |;
           |
           |load solr.`solr1/mlsql_example` as mlsql_example;
           |
           |save mlsql_example_data as solr.`solr1/mlsql_example`
           |options soft_commit_secs = "1";
           |
         """.stripMargin, sq)

      //solr commit time is 1 secs,so need sleep
      Thread.sleep(2000)

      ScriptSQLExec.parse("load solr.`solr1/mlsql_example` as mlsql_example;", sq)

      assume(spark.sql("select * from mlsql_example").collect().last.get(0) == "1")
    }
  }

  val server = new streaming.test.servers.SolrServer()

  override protected def beforeAll(): Unit = {
    server.startServer
  }

  override protected def afterAll(): Unit = {
    server.stopServer
  }
}
