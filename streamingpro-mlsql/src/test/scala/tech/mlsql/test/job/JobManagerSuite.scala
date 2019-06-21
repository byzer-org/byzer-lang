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

  def waitJobStarted(groupId: String, timeoutSec: Long = 10) = {
    var count = timeoutSec
    while (JobManager.getJobInfo.filter(f => f._1 == groupId) == 0 && count > 0) {
      Thread.sleep(1000)
      count -= 1
    }
    count > 0
  }

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

      // start a stream job and then kill it
      val groupRef2 = new AtomicReference[String]()
      executeCodeWithGroupId(runtime, groupRef2, JobManagerSuiteData.streamCode)
      assert(waitJobStarted(groupRef2.get()))

      executeCode(runtime,
        s"""
           |!kill ${groupRef2.get()};
        """.stripMargin)
      Thread.sleep(2000)
      assert(!checkJob(groupRef2.get()))
    }
  }
}

object JobManagerSuiteData {
  val streamCode =
    """
      |-- the stream name, should be uniq.
      |set streamName="streamExample";
      |
      |
      |-- mock some data.
      |set data='''
      |{"key":"yes","value":"no","topic":"test","partition":0,"offset":0,"timestamp":"2008-01-24 18:01:01.001","timestampType":0}
      |{"key":"yes","value":"no","topic":"test","partition":0,"offset":1,"timestamp":"2008-01-24 18:01:01.002","timestampType":0}
      |{"key":"yes","value":"no","topic":"test","partition":0,"offset":2,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
      |{"key":"yes","value":"no","topic":"test","partition":0,"offset":3,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
      |{"key":"yes","value":"no","topic":"test","partition":0,"offset":4,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
      |{"key":"yes","value":"no","topic":"test","partition":0,"offset":5,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
      |''';
      |
      |-- load data as table
      |load jsonStr.`data` as datasource;
      |
      |-- convert table as stream source
      |load mockStream.`datasource` options
      |stepSizeRange="0-3"
      |as newkafkatable1;
      |
      |-- aggregation
      |select cast(key as string) as k,count(*) as c  from newkafkatable1 group by key
      |as table21;
      |
      |-- output the the result to console.
      |save append table21
      |as console.``
      |options mode="Complete"
      |and duration="15"
      |and checkpointLocation="/tmp/cpl3";
      |
    """.stripMargin
}
