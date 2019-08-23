package tech.mlsql.test.job

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import tech.mlsql.job.JobManager
import tech.mlsql.test.JobManagerSuiteData

/**
  * 2019-06-21 WilliamZhu(allwefantasy@gmail.com)
  */
class JobManagerSuite extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {


  "Job manager" should "kill batch job/stream successfully" in {
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

      assert(JobManager.getJobInfo.size > 0)


      assert(checkJob(runtime, groupId))

      executeCode(runtime,
        s"""
           |!kill ${groupId};
        """.stripMargin)
      Thread.sleep(2000)
      assert(!checkJob(runtime, groupId))

      // start a stream job and then kill it
      val groupRef2 = new AtomicReference[String]()
      executeCodeWithGroupId(runtime, groupRef2, JobManagerSuiteData.streamCode)
      assert(waitJobStarted(groupRef2.get()))

      executeCode(runtime,
        s"""
           |!kill ${groupRef2.get()};
        """.stripMargin)
      Thread.sleep(2000)
      assert(!checkJob(runtime, groupRef2.get()))
    }
  }

//  "Job manager" should "init job" in {
  //    withContext(setupBatchContext(batchParamsWithJsonFile("init"))) { runtime: SparkRuntime =>
  //      executeCode(runtime, "select * from table1 as output;")
  //    }
  //  }
}


