package streaming.test.datasource

import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.log.Logging

/**
  * 2018-12-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MongoSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll with Logging {
  "load mongo" should "work fine" in {

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
           |save overwrite data1 as mongo.`twitter/cool` where
           |    partitioner="MongoPaginateBySizePartitioner"
           |and uri="mongodb://127.0.0.1:27017/twitter";
           |
           |load mongo.`twitter/cool` where
           |    partitioner="MongoPaginateBySizePartitioner"
           |and uri="mongodb://127.0.0.1:27017/twitter"
           |as table1;
           |select * from table1 as output1;
           |
           |connect mongo where
           |    partitioner="MongoPaginateBySizePartitioner"
           |and uri="mongodb://127.0.0.1:27017/twitter" as mongo_instance;
           |
           |load mongo.`mongo_instance/cool`
           |as table1;
           |select * from table1 as output2;
           |
           |load mongo.`cool` where
           |    partitioner="MongoPaginateBySizePartitioner"
           |and uri="mongodb://127.0.0.1:27017/twitter"
           |as table1;
           |select * from table1 as output3;
         """.stripMargin, sq)
      assume(spark.sql("select jack from output1").collect().last.get(0) == "cool")
      assume(spark.sql("select jack from output2").collect().last.get(0) == "cool")
      assume(spark.sql("select jack from output3").collect().last.get(0) == "cool")
    }
  }

  val server = new streaming.test.servers.MongoServer("4.0")

  override protected def beforeAll(): Unit = {
    server.startServer
  }

  override protected def afterAll(): Unit = {
    server.stopServer
  }
}
