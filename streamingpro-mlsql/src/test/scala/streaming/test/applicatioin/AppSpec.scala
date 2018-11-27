package streaming.test.applicatioin

import org.apache.spark.sql.AnalysisException
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.{BasicMLSQLConfig, SpecFunctions}

class AppSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {


  "streaming.mode.application.fails_all" should "work fine" in {
    println("set streaming.mode.application.fails_all true")
    intercept[AnalysisException] {
      appWithBatchContext(batchParams ++ Array(
        "-streaming.mode.application.fails_all", "true"
      ), "classpath:///test/batch-mlsql-error.json")
    }

    println("set streaming.mode.application.fails_all false but there are exception")
    appWithBatchContext(batchParams ++ Array(
      "-streaming.mode.application.fails_all", "false"
    ), "classpath:///test/batch-mlsql-error.json")

    println("set streaming.mode.application.fails_all true but there are no exception")
    appWithBatchContext(batchParams ++ Array(
      "-streaming.mode.application.fails_all", "true"
    ), "classpath:///test/batch-mlsql.json")

  }

}
