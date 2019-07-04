package tech.mlsql.test.stream

import java.io.File
import java.util.concurrent.atomic.AtomicReference

import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import tech.mlsql.test.StreamSubBatchQuerySuiteData

/**
  * 2019-06-21 WilliamZhu(allwefantasy@gmail.com)
  */
class StreamSubBatchQuerySuite extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {


  "Stream" should "stream job sub batch query" in {
    withContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime =>
      val groupRef2 = new AtomicReference[String]()
      val spark = getSession(runtime)
      executeStreamCode(runtime, StreamSubBatchQuerySuiteData.streamCode(
        """
          |select count(*) as c from jack as newjack;
          |save append newjack as delta.`/tmp/jack`;
        """.stripMargin))
      assert(waitJobStartedByName("streamExample"))
      val file = new File(ScriptSQLExec.context().home + "/tmp/jack")
      FileUtils.forceMkdir(file)

      val res = waitWithCondition(() => {
        val res = executeCode(runtime,
          """
            |load delta.`/tmp/jack` as table1;
          """.stripMargin)
        res != null && res.size > 0
      }, 30)

      assert(res)

    }
  }

}


