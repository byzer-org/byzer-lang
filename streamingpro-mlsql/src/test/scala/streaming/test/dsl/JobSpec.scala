package streaming.test.dsl

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import tech.mlsql.job.{JobManager, MLSQLJobType}

/**
  * 2019-01-30 WilliamZhu(allwefantasy@gmail.com)
  */
class JobSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {
  "set grammar" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      var sq = createSSEL
      JobManager.initForTest(spark)
      try {
        (0 until 100).foreach { m =>
          new Thread(new Runnable {
            override def run(): Unit = {
              val job = JobManager.getJobInfo("jack", MLSQLJobType.SCRIPT, "jack1", "", -1)
              println(job.groupId)
            }
          }).start()
        }
      } finally {
        JobManager.shutdown
      }
    }
  }

}
