package streaming.test.datasource

import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.log.Logging

/**
  * 2018-12-20 WilliamZhu(allwefantasy@gmail.com)
  */
class ElasticSearchSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll with Logging {
  "load es" should "[es] work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      var sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |
           |set data='''
           |{"jack":"cool"}
           |''';
           |
           |load jsonStr.`data` as data1;
           |
           |save overwrite data1 as es.`twitter/cool` where
           |`es.index.auto.create`="true"
           |and es.nodes.wan.only="true"
           |and es.nodes="127.0.0.1";
           |
           |load es.`twitter/cool` where
           |and es.nodes="127.0.0.1"
           |and es.nodes.wan.only="true"
           |as table1;
           |select * from table1 as output1;
           |
           |connect es where  `es.index.auto.create`="true"
           |and es.nodes.wan.only="true"
           |and es.nodes="127.0.0.1" as es_instance;
           |
           |load es.`es_instance/twitter/cool`
           |as table1;
           |select * from table1 as output2;
           |
           |
         """.stripMargin, sq)
      assume(spark.sql("select * from output1").collect().last.get(0) == "cool")
      assume(spark.sql("select * from output2").collect().last.get(0) == "cool")
    }
  }

  val server = new streaming.test.servers.ElasticSearchServer("6.5.3")

  override protected def beforeAll(): Unit = {
    server.startServer
  }

  override protected def afterAll(): Unit = {
    server.stopServer
  }
}
