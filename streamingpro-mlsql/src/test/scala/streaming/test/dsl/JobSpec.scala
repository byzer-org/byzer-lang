package streaming.test.dsl

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions, StreamingproJobManager, StreamingproJobType}

/**
  * 2019-01-30 WilliamZhu(allwefantasy@gmail.com)
  */
class JobSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {
  "set grammar" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      var sq = createSSEL
      StreamingproJobManager.initForTest(spark)
      try {
        (0 until 100).foreach { m =>
          new Thread(new Runnable {
            override def run(): Unit = {
              val job = StreamingproJobManager.getStreamingproJobInfo("jack", StreamingproJobType.SCRIPT, "jack1", "", -1)
              println(job.groupId)
            }
          }).start()
        }
      } finally {
        StreamingproJobManager.shutdown
      }
    }
  }

}
