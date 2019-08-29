package tech.mlsql.test.tf

import java.io.File

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}

import scala.io.Source

/**
  * 2019-08-19 WilliamZhu(allwefantasy@gmail.com)
  */
class TFSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  def getHome = {
    getClass.getResource("").getPath.split("streamingpro\\-mlsql").head
  }

  def getExampleProject() = {
    //sklearn_elasticnet_wine
    getHome + "examples/tf"
  }

  "tf" should "spec" in {
    withContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime =>
      copySampleLibsvmData
      val source = Source.fromFile(new File(getExampleProject + "/tf_demo.py")).getLines().mkString("\n")
      writeStringToFile("/tmp/william/tmp/tf/tf_demo.py", source)
      executeCode(runtime, TFExmaple.code("/tmp/tf/tf_demo/model", "/tmp/william/tmp/tf/tf_demo.py"))
      println()
    }
  }

  object TFExmaple {
    def code(modePath: String, scriptPath: String) =
      s"""
         |load libsvm.`sample_libsvm_data.txt` as data;
         |
         |train data as DTF.`${modePath}`
         |where
         |pythonScriptPath="${scriptPath}"
         |and  keepVersion="true"
         |and `fitParam.0.psNum`="1"
         |and PYTHON_ENV="streamingpro-spark-2.4.x"
         |;
    """.stripMargin
  }

}
