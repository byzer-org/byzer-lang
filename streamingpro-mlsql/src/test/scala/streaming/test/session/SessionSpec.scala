package streaming.test.session

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions, StreamingproJobManager, StreamingproJobType}
import streaming.session.SessionIdentifier

/**
  * Created by aston on 2018/11/2.
  */
class SessionSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  "session close" should "work fine" in {
    withBatchContext(setupBatchContext(batchParamsWithoutHive, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      StreamingproJobManager.init(runtime.sparkSession.sparkContext)
      runtime.getSession("latincross")

      assume(runtime.sessionManager.getOpenSessionCount == 1)

      runtime.sessionManager.closeSession(SessionIdentifier("latincross"))
      assume(runtime.sessionManager.getOpenSessionCount == 0)

      val jobInfo = StreamingproJobManager.getStreamingproJobInfo(
        "latincross", StreamingproJobType.SCRIPT, "test", "select sleep(5000) as output;", -1L
      )

      StreamingproJobManager.asyncRun(runtime.getSession("latincross"), jobInfo, () => {
        runtime.sparkSession.sql("select sleep(50000)").count()
      })

      assume(runtime.sessionManager.getOpenSessionCount == 1)

      while(StreamingproJobManager.getJobInfo.size == 0){
        Thread.sleep(1000)
      }

      StreamingproJobManager.killJob(StreamingproJobManager.getJobInfo.head._2.groupId)

      runtime.sessionManager.closeSession(SessionIdentifier("latincross"))
      assume(runtime.sessionManager.getOpenSessionCount == 0)
      StreamingproJobManager.shutdown
    }
  }
}
