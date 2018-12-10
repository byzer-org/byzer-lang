package streaming.test.rest

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Bootstrap
import net.csdn.common.collections.WowCollections
import net.csdn.junit.BaseControllerTest
import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions, StreamingproJobManager}

/**
  * 2018-12-06 WilliamZhu(allwefantasy@gmail.com)
  */
class RestAPISpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll {


  "/run/script" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      mockServer

      StreamingproJobManager.init(spark.sparkContext)
      val controller = new BaseControllerTest()

      val response = controller.get("/run/script", WowCollections.map(
        "sql", "select 1 as a as t;"
      ));
      assume(response.status() == 200)
      assume(response.originContent() == "[{\"a\":1}]")
      StreamingproJobManager.shutdown
    }
  }

  "/run/script auth" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      mockServer

      StreamingproJobManager.init(spark.sparkContext)
      val controller = new BaseControllerTest()

      val path = this.getClass().getClassLoader().getResource("").getPath()
        .replace("test-" ,"")

      val response = controller.get("/run/script", WowCollections.map(
        "sql", s"include hdfs.`${path}/test/include-set.txt` ;select '$${xx}' as a as t;"
        ,"skipAuth" ,"false"
        ,"owner" ,"latincross"
      ));
      assume(response.status() == 200)
      assume(response.originContent() == "[{\"a\":\"latincross\"}]")
      StreamingproJobManager.shutdown
    }
  }
}
