package streaming.core

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.ScriptSQLExec

/**
  * Created by allwefantasy on 26/5/2018.
  */
class PythonMLSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {
  copySampleLibsvmData

  "sklearn-multi-model" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("sklearn-multi-model-trainning"), sq)
      spark.read.parquet("/tmp/william/tmp/model/0").show()
    }
  }

  "sklearn-multi-model-with-sample" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("sklearn-multi-model-trainning-with-sample"), sq)
      spark.read.parquet("/tmp/william/tmp/model/0").show()
    }
  }

  "tensorflow-cnn-model" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("tensorflow-cnn"), sq)

    }
  }
}
