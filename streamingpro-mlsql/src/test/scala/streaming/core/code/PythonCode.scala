package streaming.core.code

/**
  * Created by allwefantasy on 12/7/2018.
  */
object PythonCode {
  val pythonTrainCode =
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

  val pythonCodeEnableLocal =
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

  val pythonCodeFail =
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
      |assert(1==2)
      |
    """.stripMargin

  val pythonPredictCode =
    """
      |from pyspark.ml.linalg import VectorUDT, Vectors
      |import pickle
      |import python_fun
      |
      |def predict(index, s):
      |    items = [i for i in s]
      |    feature = VectorUDT().deserialize(pickle.loads(items[0]))
      |    print(pickle.loads(items[1])[0])
      |    model = pickle.load(open(pickle.loads(items[1])[0]+"/model.pickle"))
      |    y = model.predict([feature.toArray()])
      |    print("------".format)
      |    return [VectorUDT().serialize(Vectors.dense(y))]
      |
      |
      |python_fun.udf(predict)
      | """.stripMargin

  val pythonBadPredictCode =
    """
      |from pyspark.ml.linalg import VectorUDT, Vectors
      |import pickle
      |import python_fun
      |
      |def predict(index, s):
      |    items = [i for i in s]
      |    feature = VectorUDT().deserialize(pickle.loads(items[0]))
      |    print(pickle.loads(items[1])[0])
      |    model = pickle.load(open(pickle.loads(items[1])[0]+"/model.pickle"))
      |    y = model.predict([feature.toArray()])
      |    print("----------".format)
      |    return [VectorUDT().serialize(Vectors.dense(y))]
      |
      |
      |python_fun.udf(predict)
      | """.stripMargin

  val pythonBatchPredictCode =
    """
      |from pyspark.ml.linalg import VectorUDT, Vectors
      |import pickle
      |import os
      |import mlsql
      |import json
      |
      |tempDataLocalPath = mlsql.internal_system_param["tempDataLocalPath"]
      |tempModelLocalPath = mlsql.internal_system_param["tempModelLocalPath"]
      |tempResultLocalPath = mlsql.internal_system_param["tempResultLocalPath"]
      |
      |print(tempDataLocalPath)
      |files = [file for file in os.listdir(tempDataLocalPath) if file.endswith(".json")]
      |res = []
      |for file in files:
      |    with open(tempDataLocalPath + "/" + file) as f:
      |        for line in f.readlines():
      |            obj = json.loads(line)
      |            f_size = obj["features"]["size"]
      |            f_indices = obj["features"]["indices"]
      |            f_values = obj["features"]["values"]
      |            res.append(Vectors.sparse(f_size, f_indices, f_values).toArray())
      |model = pickle.load(open(tempModelLocalPath+"/_model_0/model/0//model.pickle"))
      |model.predict(res)
      |
      |if not os.path.exists(tempResultLocalPath):
      |    os.makedirs(tempResultLocalPath)
      |
      |with open(tempResultLocalPath+"/0.json","w") as f:
      |    f.write("{}")
      | """.stripMargin
}
