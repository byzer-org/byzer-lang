### 离线批量预测

[PythonAlg](./en/mlsql-python-machine-learning.md)训练好的算法模型可以利用批量据测的模块对离线数据进行预测。

### Example

我们以训练`Kmeans`模型为例，通过下列脚本，`PythonAlg`模块会训练出一个`Kmeans`模型。

```sql
set dataPath = "/Users/fchen/tmp/streamingpro/python/shunzi/train.data2";

load json.`${dataPath}` as transform_scale_data;

-- KMeans训练 

-- train configuration
set sklearnPredictPath="/user/zhuhl/mlsql/algs/sklearn/predict.py";
set trainPythonPath="/Users/fchen/tmp/streamingpro/python/shunzi/train.py";
set modelPath="/tmp/user_recgn/models/IsolationForest_v2";


set kafkaDomain = "localhost:9002";

train transform_scale_data as PythonAlg.`${modelPath}` 
where pythonScriptPath="${trainPythonPath}"
  and `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
  and `kafkaParam.topic`="test"
  and `kafkaParam.group_id`="g_test-1"
-- distribute data
and  enableDataLocal="true"
and  dataLocalFormat="json"
-- and distributeEveryExecutor="true"
-- sklearn params

and `fitParam.0.moduleName`="sklearn.cluster"
and `fitParam.0.className`="KMeans"
and `fitParam.0.featureCol`="features"
and `fitParam.0.n_clusters` = "12"
and `systemParam.pythonPath`="/usr/local/share/anaconda3/envs/mlsql-2.2.0/bin/python"
and `systemParam.pythonVer`="3.6";

```

通过`PythonAlg`训练完成，我们在模型路径`/tmp/user_recgn/models/IsolationForest_v2`下生成对应的模型，我们只需要将模型路径传递给`BatchPythonAlg`即可进行批量预测。

```sql
  set dataPath2 = "/path/to/train/data";
  load json.`${dataPath}` as transform_scale_data;
  
  -- train configuration
  set kafkaDomain="kafka31:9092";
  set sklearnPredictPath="/Users/fchen/tmp/streamingpro/python/shunzi/predict2.py";
  -- PythonAlg训练的模型路径
  set modelPath="/tmp/user_recgn/models/IsolationForest_v2";
  
  train transform_scale_data as BatchPythonAlg.`${modelPath}` where
  pythonScriptPath="${sklearnPredictPath}"
  and `systemParam.pythonPath`="/usr/local/share/anaconda3/envs/mlsql-2.2.0/bin/python"
  and `systemParam.pythonVer`="3.6"
  and inputCol = "features2"
  -- 每次喂给模型条数
  and batchSize = "1000"
  -- 批量预测udf名
  and predictFun = "fchen_predict"
  -- 预测结果的列名
  and predictCol = "predict_vector"
  -- 预测的零时表名
  and predictTable = "fchen_result"
  and algIndex="0";
  
  select vec_array(predict_vector)[0] as predict_label,* from fchen_result
  as fchen_result1;
  
  save overwrite fchen_result1 as json.`/tmp/fchen_result`;
```

由于`BatchPythonAlg`模块需要进行批量预测，所以我们需要一个批量预测的python脚本。

```python
from __future__ import absolute_import
from pyspark.ml.linalg import MatrixUDT, Matrix, DenseMatrix
import pickle
import os

run_for_test = False
if run_for_test:
    import mlsql.python_fun
else:
    import python_fun


def predict(index, s):
    items = [i for i in s]
    modelPath = pickle.loads(items[1])[0] + "/model.pkl"

    if not hasattr(os, "mlsql_models"):
        setattr(os, "mlsql_models", {})
    if modelPath not in os.mlsql_models:
        print("Load sklearn model %s" % modelPath)
        os.mlsql_models[modelPath] = pickle.load(open(modelPath, "rb"))

    model = os.mlsql_models[modelPath]
    rawMatrix = pickle.loads(items[0])
    feature = MatrixUDT().deserialize(rawMatrix)
    y = model.predict(feature.toArray())
    row_num = len(y)
    dm = DenseMatrix(row_num, 1, y)
    return [MatrixUDT().serialize(dm)]


python_fun.udf(predict)

```

可以看到，通过`BatchPythonAlg`模块，我们只需要训练一遍模型，既可以完成在线服务和离线批量预测的功能。

### 问题

> Q: 为什么需要批量预测模块？

> A: PythonAlg对数据是按条来处理的，如果我们数据量太大，预测效率就大大降低了，所以我们需要批量预测接口。

---

> Q: 有什么需要注意的？

> A: Python模型需要支持批量预测。
