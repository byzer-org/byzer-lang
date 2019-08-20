package streaming.test.datasource

import net.csdn.common.collections.WowCollections
import net.csdn.junit.BaseControllerTest
import net.sf.json.{JSONArray, JSONObject}
import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.job.JobManager

import scala.collection.JavaConverters._


/**
  * 2019-01-11 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLSystemSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll with Logging {
  "load jobs" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      mockServer
      JobManager.initForTest(spark)
      val t = new Thread(new Runnable {
        override def run(): Unit = {
          val controller = new BaseControllerTest()
          val response = controller.get("/run/script", WowCollections.map(
            "sql", "select sleep(5000) as a as t;",
            "owner", "jack"
          ))
        }
      })
      t.start()

      val controller = new BaseControllerTest()
      var response = controller.get("/run/script", WowCollections.map(
        "sql", "load _mlsql_.`jobs` as output;",
        "owner", "jack"
      ))

      var jobSize = JSONArray.fromObject(response.content()).asScala.filter { job =>
        job.asInstanceOf[JSONObject].getString("jobContent") == "select sleep(5000) as a as t;"
      }
      assume(jobSize.length == 1)
      val groupId = jobSize.toSeq.head.asInstanceOf[JSONObject].getString("groupId")

      controller.get("/run/script", WowCollections.map(
        "sql",
        s"""run  command as Kill.`${groupId}`;""",
        "owner", "jack"
      ))

      response = controller.get("/run/script", WowCollections.map(
        "sql", "load _mlsql_.`jobs` as output;",
        "owner", "jack"
      ))

      jobSize = JSONArray.fromObject(response.content()).asScala.filter { job =>
        job.asInstanceOf[JSONObject].getString("jobContent") == "select sleep(5000) as a as t;"
      }
      assume(jobSize.length == 0)

      JobManager.shutdown
    }
  }


}
