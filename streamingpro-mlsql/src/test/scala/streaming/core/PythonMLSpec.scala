package streaming.core

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.BasicSparkOperation
import streaming.common.shell.ShellCommand
import streaming.core.code.PythonCode
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.ScriptSQLExec
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 26/5/2018.
  */
class PythonMLSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  copySampleLibsvmData

  "sklearn-multi-model" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("sklearn-multi-model-trainning"), sq)
      val res = spark.read.parquet("/tmp/william/tmp/model/0").collect()
      assume(res.filter(f => f.getAs[String]("alg").contains("SVC")).head.getAs[Int]("algIndex") == 1)
      assume(res.filter(f => f.getAs[String]("alg").contains("Multinom")).head.getAs[Int]("algIndex") == 0)
    }
  }

  "sklearn-user-script" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      val pythonCode =
        """
          |import mlsql_model
          |import mlsql
          |from sklearn.naive_bayes import MultinomialNB
          |
          |clf = MultinomialNB()
          |
          |mlsql.sklearn_configure_params(clf)
          |
          |
          |def train(X, y, label_size):
          |    clf.partial_fit(X, y, classes=range(label_size))
          |
          |
          |mlsql.sklearn_batch_data(train)
          |
          |X_test, y_test = mlsql.get_validate_data()
          |print("cool------")
          |if len(X_test) > 0:
          |    testset_score = clf.score(X_test, y_test)
          |    print("mlsql_validation_score:%f" % testset_score)
          |
          |mlsql_model.sk_save_model(clf)
          |
        """.stripMargin
      writeStringToFile("/tmp/sklearn-user-script.py", pythonCode)
      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("sklearn-user-script"), Map(
        "pythonScriptPath" -> "/tmp/sklearn-user-script.py"
      )), sq)
      spark.read.parquet("/tmp/william/tmp/model/0").show()
    }
  }

  "python-alg-script" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL

      writeStringToFile("/tmp/sklearn-user-script.py", PythonCode.pythonTrainCode)
      writeStringToFile("/tmp/sklearn-user-predict-script.py", PythonCode.pythonPredictCode)
      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("python-alg-script"), Map(
        "pythonScriptPath" -> "/tmp/sklearn-user-script.py",
        "pythonPredictScriptPath" -> "/tmp/sklearn-user-predict-script.py"
      )), sq)
      spark.sql("select * from newdata").show()
    }
  }

  "python-alg-script-enable-data-local" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL


      writeStringToFile("/tmp/sklearn-user-script.py", PythonCode.pythonCodeEnableLocal)
      writeStringToFile("/tmp/sklearn-user-predict-script.py", PythonCode.pythonPredictCode)
      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("python-alg-script-enable-data-local"), Map(
        "pythonScriptPath" -> "/tmp/sklearn-user-script.py",
        "keepVersion" -> "false",
        "path" -> "/tmp/pa_model",
        "distributeEveryExecutor" -> "true"

      )), sq)

      //we can change model path
      ShellCommand.exec("rm -rf /tmp/william/pa_model2")
      ShellCommand.exec("mv /tmp/william/tmp/pa_model /tmp/william/pa_model2")

      ScriptSQLExec.parse(TemplateMerge.merge(
        "register PythonAlg.`/pa_model2` as jack options\npythonScriptPath=\"${pythonPredictScriptPath}\"\n;select jack(features) from data\nas newdata;",
        Map(
          "pythonPredictScriptPath" -> "/tmp/sklearn-user-predict-script.py"
        )), sq)

      spark.sql("select * from newdata").show()
    }
  }

  "python-alg-script-enable-data-local-with-model-version" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL

      writeStringToFile("/tmp/sklearn-user-script.py", PythonCode.pythonCodeEnableLocal)
      writeStringToFile("/tmp/sklearn-user-predict-script.py", PythonCode.pythonPredictCode)

      ShellCommand.exec("rm -rf /tmp/william/pa_model_k")

      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("python-alg-script-enable-data-local"), Map(
        "pythonScriptPath" -> "/tmp/sklearn-user-script.py",
        "keepVersion" -> "true",
        "path" -> "/pa_model_k",
        "distributeEveryExecutor" -> "true"

      )), sq)

      //we can change model path
      ScriptSQLExec.parse(TemplateMerge.merge(
        "register PythonAlg.`/pa_model_k` as jack options\npythonScriptPath=\"${pythonPredictScriptPath}\"\n;select jack(features) from data\nas newdata;",
        Map(
          "pythonPredictScriptPath" -> "/tmp/sklearn-user-predict-script.py"
        )), sq)

      assume(new File("/tmp/william/pa_model_k/_model_0").exists())

      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("python-alg-script-enable-data-local"), Map(
        "pythonScriptPath" -> "/tmp/sklearn-user-script.py",
        "keepVersion" -> "true",
        "path" -> "/pa_model_k",
        "distributeEveryExecutor" -> "true"

      )), sq)

      assume(new File("/tmp/william/pa_model_k/_model_1").exists())
      spark.sql("select * from newdata").show()
    }
  }

  "python-alg-script-enable-data-local-not-distributeEveryExecutor-with-model-version" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL

      writeStringToFile("/tmp/sklearn-user-script.py", PythonCode.pythonCodeEnableLocal)
      writeStringToFile("/tmp/sklearn-user-predict-script.py", PythonCode.pythonPredictCode)

      ShellCommand.exec("rm -rf /tmp/william/pa_model_k")

      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("python-alg-script-enable-data-local"), Map(
        "pythonScriptPath" -> "/tmp/sklearn-user-script.py",
        "keepVersion" -> "true",
        "path" -> "/pa_model_k",
        "distributeEveryExecutor" -> "false"

      )), sq)

      //we can change model path
      ScriptSQLExec.parse(TemplateMerge.merge(
        "register PythonAlg.`/pa_model_k` as jack options\npythonScriptPath=\"${pythonPredictScriptPath}\"\n;select jack(features) from data\nas newdata;",
        Map(
          "pythonPredictScriptPath" -> "/tmp/sklearn-user-predict-script.py"
        )), sq)

      assume(new File("/tmp/william/pa_model_k/_model_0").exists())

      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("python-alg-script-enable-data-local"), Map(
        "pythonScriptPath" -> "/tmp/sklearn-user-script.py",
        "keepVersion" -> "true",
        "path" -> "/pa_model_k",
        "distributeEveryExecutor" -> "false"

      )), sq)

      assume(new File("/tmp/william/pa_model_k/_model_1").exists())
      spark.sql("select * from newdata").show()
    }
  }


  "python-alg-script-train-fail-should-log" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL

      writeStringToFile("/tmp/sklearn-user-script.py", PythonCode.pythonCodeFail)

      ShellCommand.exec("rm -rf /tmp/william/pa_model_k")

      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("python-alg-script-enable-data-local"), Map(
        "pythonScriptPath" -> "/tmp/sklearn-user-script.py",
        "keepVersion" -> "true",
        "path" -> "/pa_model_k",
        "distributeEveryExecutor" -> "false"

      )), sq)

      var res = spark.sql("select * from parquet.`/tmp/william/pa_model_k/_model_0/meta/0`")
      assume(res.collect().map(f => f.getAs[String]("status")).head == "fail")


      writeStringToFile("/tmp/sklearn-user-script.py", PythonCode.pythonCodeEnableLocal)
      ShellCommand.exec("rm -rf /tmp/william/pa_model_k")

      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("python-alg-script-enable-data-local"), Map(
        "pythonScriptPath" -> "/tmp/sklearn-user-script.py",
        "keepVersion" -> "true",
        "path" -> "/pa_model_k",
        "distributeEveryExecutor" -> "false"

      )), sq)

      res = spark.sql("select * from parquet.`/tmp/william/pa_model_k/_model_0/meta/0`")
      assume(res.collect().map(f => f.getAs[String]("status")).head == "success")

    }
  }


  "tt" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |load libsvm.`sample_libsvm_data.txt` as data;
           |save overwrite data as json.`/tmp/kk`;
         """.stripMargin, sq)
    }
  }

  "sklearn-multi-model-with-sample" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("sklearn-multi-model-trainning-with-sample"), sq)
      spark.read.parquet("/tmp/william/tmp/model/0").show()
    }
  }

  "tensorflow-cnn-model" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("tensorflow-cnn"), sq)
    }
  }

  "distribute-tensorflow" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParamsWithPort, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL

      writeStringToFile("/tmp/tensorflow-distribute.py", loadPythonStr("tensorflow-distribute.py"))

      ShellCommand.exec("rm -rf /tmp/william/pa_model_tf")

      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("distribute-tensorflow"), Map(
        "pythonScriptPath" -> "/tmp/tensorflow-distribute.py",
        "keepVersion" -> "true",
        "path" -> "/pa_model_tf",
        "distributeEveryExecutor" -> "false"

      )), sq)

      val res = spark.sql("select * from parquet.`/tmp/william/pa_model_tf/_model_0/meta/0`")
      res.show(false)
    }
  }
}
