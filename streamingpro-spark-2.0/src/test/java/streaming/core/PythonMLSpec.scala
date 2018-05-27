package streaming.core

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.ScriptSQLExec
import streaming.dsl.template.TemplateMerge

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

  "sklearn-user-script" should "work fine" in {
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

  "python-alg-script" should "work fine" in {
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

      val pythonPridcitCode =
        """
          |from pyspark.ml.linalg import VectorUDT, Vectors
          |import pickle
          |import python_fun
          |
          |
          |def predict(index, s):
          |    items = [i for i in s]
          |    feature = VectorUDT().deserialize(pickle.loads(items[0]))
          |    print(pickle.loads(items[1])[0])
          |    model = pickle.load(open(pickle.loads(items[1])[0]+"/model.pickle"))
          |    y = model.predict([feature.toArray()])
          |    return [VectorUDT().serialize(Vectors.dense(y))]
          |
          |
          |python_fun.udf(predict)""".stripMargin

      writeStringToFile("/tmp/sklearn-user-script.py", pythonCode)
      writeStringToFile("/tmp/sklearn-user-predict-script.py", pythonPridcitCode)
      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("python-alg-script"), Map(
        "pythonScriptPath" -> "/tmp/sklearn-user-script.py",
        "pythonPredictScriptPath" -> "/tmp/sklearn-user-predict-script.py"
      )), sq)
      spark.sql("select * from newdata").show()
    }
  }

  "python-alg-script-enable-data-local" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      val pythonCode =
        """
          |import mlsql_model
          |import mlsql
          |import os
          |import json
          |from pyspark.ml.linalg import Vectors
          |from sklearn.naive_bayes import MultinomialNB
          |
          |clf = MultinomialNB()
          |
          |mlsql.sklearn_configure_params(clf)
          |tempDataLocalPath = mlsql.internal_system_param["tempDataLocalPath"]
          |
          |print(tempDataLocalPath)
          |files = [file for file in os.listdir(tempDataLocalPath) if file.endswith(".json")]
          |res = []
          |res_label = []
          |for file in files:
          |    with open(tempDataLocalPath + "/" + file) as f:
          |        for line in f.readlines():
          |            obj = json.loads(line)
          |            f_size = obj["features"]["size"]
          |            f_indices = obj["features"]["indices"]
          |            f_values = obj["features"]["values"]
          |            res.append(Vectors.sparse(f_size, f_indices, f_values).toArray())
          |            res_label.append(obj["label"])
          |
          |
          |def train(X, y, label_size):
          |    clf.partial_fit(X, y, classes=range(label_size))
          |
          |
          |train(res,res_label,2)
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

      val pythonPridcitCode =
        """
          |from pyspark.ml.linalg import VectorUDT, Vectors
          |import pickle
          |import python_fun
          |
          |
          |def predict(index, s):
          |    items = [i for i in s]
          |    feature = VectorUDT().deserialize(pickle.loads(items[0]))
          |    print(pickle.loads(items[1])[0])
          |    model = pickle.load(open(pickle.loads(items[1])[0]+"/model.pickle"))
          |    y = model.predict([feature.toArray()])
          |    return [VectorUDT().serialize(Vectors.dense(y))]
          |
          |
          |python_fun.udf(predict)""".stripMargin

      writeStringToFile("/tmp/sklearn-user-script.py", pythonCode)
      writeStringToFile("/tmp/sklearn-user-predict-script.py", pythonPridcitCode)
      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("python-alg-script-enable-data-local"), Map(
        "pythonScriptPath" -> "/tmp/sklearn-user-script.py",
        "pythonPredictScriptPath" -> "/tmp/sklearn-user-predict-script.py"
      )), sq)
      spark.sql("select * from newdata").show()
    }
  }

  "tt" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |load libsvm.`sample_libsvm_data.txt` as data;
           |save data as json.`/tmp/kk`;
         """.stripMargin, sq)
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
