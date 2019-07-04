package tech.mlsql.test.antlrv4

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}

/**
  * 2019-07-04 WilliamZhu(allwefantasy@gmail.com)
  */
class Antlrv4Suite extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {


  "anlrv4" should "unknow statement" in {
    withContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime =>
      val caught =
        intercept[RuntimeException] {
          executeCode(runtime,
            """
              |no_sence;
              |select 1 as a as output;
            """.stripMargin)
        }
      assert(caught.getMessage.contains("mismatched input 'no_sence' expecting"))
      val items = executeCode(runtime,
        """
          |select 1 as a as output;
        """.stripMargin)
      assert(items.length == 1)
    }


  }
  

}
