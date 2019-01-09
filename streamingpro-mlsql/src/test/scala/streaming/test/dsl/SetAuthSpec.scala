package streaming.test.dsl

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{OperateType, TableType}

/**
  * 2019-01-09 WilliamZhu(allwefantasy@gmail.com)
  */
class SetAuthSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {
  "auth-set-statement" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      val ssel = createSSEL
      val mlsql =
        """
      set jack=`echo` where type="shell";
      """.stripMargin

      withClue("auth fail") {
        assertThrows[RuntimeException] {
          ScriptSQLExec.parse(mlsql, ssel, true, false, true)
        }
      }
      val loadMLSQLTable = ssel.authProcessListner.get.tables().tables.filter(f => (f.tableType == TableType.GRAMMAR && f.operateType == OperateType.SET))
      assume(loadMLSQLTable.size == 1)
    }

  }
}
