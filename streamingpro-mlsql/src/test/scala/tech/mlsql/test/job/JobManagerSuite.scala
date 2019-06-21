package tech.mlsql.test.job

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import tech.mlsql.job.JobManager

/**
  * 2019-06-21 WilliamZhu(allwefantasy@gmail.com)
  */
class JobManagerSuite extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  "Job manager" should "kill job successfully" in {
    withContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime =>
      val groupRef = new AtomicReference[String]()
      executeCodeWithGroupIdAsync(runtime, groupRef,
        """
          |select sleep(60000) as a as output;
        """.stripMargin)

      var count = 10
      while (JobManager.getJobInfo.size == 0 && count > 0) {
        Thread.sleep(1000)
        count -= 1
      }
      val groupId = groupRef.get()
      println(JobManager.getJobInfo.toList)
      println(groupId)
      assert(JobManager.getJobInfo.size > 0)

      def checkJob(groupId: String) = {
        val items = executeCode(runtime,
          """
            |!show jobs;
          """.stripMargin)
        items.filter(r => r.getAs[String]("groupId") == groupId).length == 1
      }

      assert(checkJob(groupId))

      executeCode(runtime,
        s"""
           |!kill ${groupId};
        """.stripMargin)
      Thread.sleep(2000)
      assert(!checkJob(groupId))

    }
  }
}
