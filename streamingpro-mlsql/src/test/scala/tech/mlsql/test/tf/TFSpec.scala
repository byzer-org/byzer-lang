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
    }
  }

  object TFExmaple {
    def code(modePath: String, scriptPath: String) =
      s"""
         |load libsvm.`sample_libsvm_data.txt` as data;
         |
         |train data as DistributedTensorflow.`${modePath}`
         |where
         |pythonScriptPath="${scriptPath}"
         |and  keepVersion="true"
         |
         |and  enableDataLocal="true"
         |and  dataLocalFormat="json"
         |
         |and  `fitParam.0.jobName`="worker"
         |and  `fitParam.0.taskIndex`="0"
         |
         |and  `fitParam.1.jobName`="worker"
         |and  `fitParam.1.taskIndex`="1"
         |
         |and  `fitParam.2.jobName`="ps"
         |and  `fitParam.2.taskIndex`="0"
         |;
    """.stripMargin
  }

}
