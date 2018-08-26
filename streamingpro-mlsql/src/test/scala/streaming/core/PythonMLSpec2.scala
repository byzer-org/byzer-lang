package streaming.core

import java.io.File

import org.apache.spark.APIDeployPythonRunnerEnv
import org.apache.spark.streaming.BasicSparkOperation
import streaming.common.shell.ShellCommand
import streaming.core.code.PythonCode
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.ScriptSQLExec
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 26/5/2018.
  */
class PythonMLSpec2 extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  copySampleLibsvmData

  "SQLPythonAlgBatchPrediction" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL

      writeStringToFile("/tmp/sklearn-user-script.py", PythonCode.pythonCodeEnableLocal)
      writeStringToFile("/tmp/sklearn-user-predict-script.py", PythonCode.pythonBatchPredictCode)

      ShellCommand.exec("rm -rf /tmp/william/pa_model_k")

      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("python-alg-script-enable-data-local"), Map(
        "pythonScriptPath" -> "/tmp/sklearn-user-script.py",
        "keepVersion" -> "true",
        "path" -> "/pa_model_k",
        "distributeEveryExecutor" -> "false"

      )), sq)

      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("python-alg-batch-predict"), Map(
        "pythonScriptPath" -> "/tmp/sklearn-user-predict-script.py",
        "modelPath" -> "/pa_model_k"
      )), sq)


    }
  }

}
