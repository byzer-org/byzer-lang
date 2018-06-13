package streaming.core

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.MLSQLRuntime
import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 4/6/2018.
  */
class SessionSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {
  "session-test" should "work fine" in {
    withMLSQLContext(setupMLSQLContext(mlsqlParams, "classpath:///test/empty.json")) { runtime: MLSQLRuntime =>
      val sessionManager = runtime.getSessionManager
      val identify = sessionManager.openSession("jack", "", "", sessionManager.conf.getAll.toMap, runtime.params.toMap, true)
      val session = sessionManager.getSession(identify)
      println(session.sql("select 1", null, "", 30))
      assume(UserGroupInformation.getCurrentUser.getShortUserName == "jack")
      sessionManager.closeSession(identify)
    }
  }
}
