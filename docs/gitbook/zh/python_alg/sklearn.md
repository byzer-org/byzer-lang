# 集成SKlearn示例

使用Python做机器学习,你只要提供一个Python项目即可。我们先看我们的主脚本。

> 该项目完整版本在MLSQL的[examples/sklearn_example_2]() 中

```sql
-- create test data
set jsonStr='''
{"features":[5.1,3.5,1.4,0.2],"label":0.0},
{"features":[5.1,3.5,1.4,0.2],"label":1.0}
{"features":[5.1,3.5,1.4,0.2],"label":0.0}
{"features":[4.4,2.9,1.4,0.2],"label":0.0}
{"features":[5.1,3.5,1.4,0.2],"label":1.0}
{"features":[5.1,3.5,1.4,0.2],"label":0.0}
{"features":[5.1,3.5,1.4,0.2],"label":0.0}
{"features":[4.7,3.2,1.3,0.2],"label":1.0}
{"features":[5.1,3.5,1.4,0.2],"label":0.0}
{"features":[5.1,3.5,1.4,0.2],"label":0.0}
''';
load jsonStr.`jsonStr` as data;
select vec_dense(features) as features ,label as label from data
as data1;


train data1 as PythonAlg.`/tmp/model` where
pythonScriptPath="/Users/allwefantasy/CSDNWorkSpace/streamingpro-spark-2.4.x/examples/sklearn_example_2"
and fitParam.0.featureCol="features"
and fitParam.0.labelCol="label"
and fitParam.0.moduleName="sklearn.svm"
and fitParam.0.className="SVC"; 
```

大家只要看PythonAlg模块即可，在这类我们可以看到，我们需要指定一个python的项目地址，然后改python项目会读取我们设置的一些参数，比如featureCol等。
这里我们准备使用SVC做训练。

> 当使用Python做机器学习的时候，我们只允许传入的数据为向量。大家可以使用特征工程里ET把数据转化为向量。

接着我们要创建一个python项目,该项目结构如下：

```
(PyMLSQL) [w@me sklearn_example_2]$ tree
.
├── MLproject
├── conda.yaml

├── batchPredict.py
├── predict.py
├── train.py
```

> PythonAlg 也支持直接把所有东西写在MLSQL脚本中。这里展示了另外一种途径，也就是运行
> HDFS上项目的方式。

大家可以看到，有两个描述问价你，有三个入口。通常机器学习里，我们需要三个入口：

1. 训练脚本入口
2. 批量预测脚本入口
3. API脚本入口（追求性能）

现在我们看看MLproject内容：

```yaml
name: tutorial

conda_env: conda.yaml

entry_points:
  main:
    train:
        command: "python train.py"
    batch_predict:
        command: "python batchPredict.py"
    api_predict:
        command: "python predict.py"
```   

在MLproject里我们配置了三个入口。大家可以根据要求进行删减。

接着我们要描述环境依赖，这里用conda.yaml,对应的内容为：

```yaml
name: tutorial
dependencies:
  - python=3.6
  - pip:
    - numpy==1.14.3
    - kafka==1.4.3
    - pyspark==2.4.0
    - pandas==0.22.0
    - scikit-learn==0.19.1
    - scipy==1.1.0
```

这里注意，kafka,pyspark是必须的，其他根据需求，比如我们示例里会用到scikit-learn，所以需要配置。

现在可以开始写Python脚本了

## 训练脚本

train.py的内容如下：

```python
from __future__ import absolute_import
import mlsql
import numpy as np
import os
import json
import sys
import pickle
import scipy.sparse as sp
import importlib
from pyspark.mllib.linalg import Vectors, SparseVector

if sys.version < '3':
    import cPickle as pickle

else:
    import pickle

    xrange = range

unicode = str

def param(key, value):
    if key in mlsql.fit_param:
        res = mlsql.fit_param[key]
    else:
        res = value
    return res

featureCol = param("featureCol", "features")
labelCol = param("labelCol", "label")
moduleName = param("moduleName", "sklearn.svm")
className = param("className", "SVC")
batchSize=1

def load_sparse_data():
    tempDataLocalPath = mlsql.internal_system_param["tempDataLocalPath"]
    # train the model on the new data for a few epochs
    datafiles = [file for file in os.listdir(tempDataLocalPath) if file.endswith(".json")]
    row_n = []
    col_n = []
    data_n = []
    y = []
    feature_size = 0
    row_index = 0
    for file in datafiles:
        with open(tempDataLocalPath + "/" + file) as f:
            for line in f.readlines():
                obj = json.loads(line)
                fc = obj[featureCol]
                if "size" not in fc and "type" not in fc:
                    feature_size = len(fc)
                    dic = [(i, a) for i, a in enumerate(fc)]
                    sv = SparseVector(len(fc), dic)
                elif "size" not in fc and "type" in fc and fc["type"] == 1:
                    values = fc["values"]
                    feature_size = len(values)
                    dic = [(i, a) for i, a in enumerate(values)]
                    sv = SparseVector(len(values), dic)

                else:
                    feature_size = fc["size"]
                    sv = Vectors.sparse(fc["size"], list(zip(fc["indices"], fc["values"])))

                for c in sv.indices:
                    row_n.append(row_index)
                    col_n.append(c)
                    data_n.append(sv.values[list(sv.indices).index(c)])

                if type(obj[labelCol]) is list:
                    y.append(np.array(obj[labelCol]).argmax())
                else:
                    y.append(obj[labelCol])
                row_index += 1
                if row_index % 10000 == 0:
                    print("processing lines: %s, values: %s" % (str(row_index), str(len(row_n))))
                    # sys.stdout.flush()
    print("X matrix : %s %s  row_n:%s col_n:%s classNum:%s" % (
        row_index, feature_size, len(row_n), len(col_n), ",".join([str(i) for i in list(set(y))])))
    sys.stdout.flush()
    return sp.csc_matrix((data_n, (row_n, col_n)), shape=(row_index, feature_size)), y

def create_alg(module_name, class_name):
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    return class_()

model = create_alg(moduleName, className)

X, y = load_sparse_data()
model.fit(X, y)

tempModelLocalPath = mlsql.internal_system_param["tempModelLocalPath"]

if not os.path.exists(tempModelLocalPath):
    os.makedirs(tempModelLocalPath)

model_file_path = tempModelLocalPath + "/model.pkl"
print("Save model to %s" % model_file_path)
pickle.dump(model, open(model_file_path, "wb"))
```

更通用化的脚本可以参看[examples/sklearn](https://github.com/allwefantasy/mlsql/blob/master/examples/sklearn).

最后的输出为：

```
modelPath   algIndex  alg  score  status  startTime  endTime  trainParams  execDesc
/tmp/model/_model_6/model/0	0		0	success	1547283455025	1547283455976	{"className":"SVC","featureCol":"features","moduleName":"sklearn.svm","labelCol":"label"}	....
```

在书写脚本的时候，需要引入mlsql模块，通过该模块，你可以获得输入数据：

```python
tempDataLocalPath = mlsql.internal_system_param["tempDataLocalPath"]
```

获得模型的保存目录：

```python
tempModelLocalPath = mlsql.internal_system_param["tempModelLocalPath"]
```

同时获得MLSQL中配置的参数方式为：

```python
def param(key, value):
    if key in mlsql.fit_param:
        res = mlsql.fit_param[key]
    else:
        res = value
    return res


featureCol = param("featureCol", "features")
labelCol = param("labelCol", "label")
moduleName = param("moduleName", "sklearn.svm")
className = param("className", "SVC")
```

这是你的脚本需要和MLSQL交互的三个地方，获得输入，输出目录，并且得到配置参数。


另外，脚本有错误的话，大家一定要细看，日志里会有类似下面这些内容：

```
[2019-01-12 ....python.PythonTrain] [owner] [allwefantasy@gmail.com] 
[2019-01-12 ....python.PythonTrain] [owner] [allwefantasy@gmail.com] params from parent: {'fitParam': {'moduleName': 'sklearn.svm', 'labelCol': 'label', 'modelPath': '/tmp/model', 'className': 'SVC', 'featureCol': 'features'}, 'internalSystemParam': {'stopFlagNum': -1, 'tempModelLocalPath': '/tmp/__mlsql__/models/2793e01827817c23908ce9a0295ef8a1/0', 'tempDataLocalPath': '/tmp/__mlsql__/data/2793e01827817c23908ce9a0295ef8a1/0', 'resource': {}}, 'systemParam': {}}
[2019-01-12 ....python.PythonTrain] [owner] [allwefantasy@gmail.com] Traceback (most recent call last):
[2019-01-12 ....python.PythonTrain][owner] [allwefantasy@gmail.com]   File "train.py", line 76, in <module>
[2019-01-12 ....python.PythonTrain][owner] [allwefantasy@gmail.com]     model.partial_fit(X, y, [i for i in xrange(labelSize)])
[2019-01-12 ....python.PythonTrain][owner] [allwefantasy@gmail.com] AttributeError: 'SVC' object has no attribute 'partial_fit'
[2019-01-12 ....python.PythonTrain][owner] [allwefantasy@gmail.com] Subprocess exited with status 1. Command ran: bash -c source activate mlflow-c121e4a8718add66591a1b0d1e1498f9ebe9118e && python train.py
[2019-01-12 ....python.PythonTrain][owner] [allwefantasy@gmail.com] java.lang.IllegalStateException: Subprocess exited with status 1. Command ran: bash -c source activate mlflow-c121e4a8718add66591a1b0d1e1498f9ebe9118e && python train.py
```

这里表示SVC没有partial_fit方法，只有fit方法。

到目前为止，我们成功的完成了训练过程。相信你也会和我一样，训练成功。

## 批量预测

很多时候，我们得到模型后，我们希望能够对历史数据进行批量预测填充。我们先看如何在MLSQL中使用者一功能特性。

```sql
predict data1 as PythonAlg.`/tmp/model` as output;
```

这表示，我们希望对data1表进行预测，并且使用/tmp/model里的模型，该模型是前面我们被训练出来的，最后预测的结果，存放在output表中。

我们来看看对一个的batchPredict.py如何书写：

```python
import mlsql
import pickle
import json
import os
from pyspark.ml.linalg import VectorUDT, Vectors
from pyspark.mllib.linalg import SparseVector

# get information from mlsql
isp = mlsql.params()["internalSystemParam"]
tempDataLocalPath = isp["tempDataLocalPath"]
tempModelLocalPath = isp["tempModelLocalPath"]
tempOutputLocalPath = isp["tempOutputLocalPath"]

model = pickle.load(open(tempModelLocalPath + "/model.pkl", "rb"))

def param(key, value):
    if key in mlsql.params()["fitParams"]:
        res = mlsql.params()["fitParams"][key]
    else:
        res = value
    return res


featureCol = param("featureCol", "features")

def parse(line):
    obj = json.loads(line)
    fc = obj[featureCol]
    if "size" not in fc and "type" not in fc:
        feature_size = len(fc)
        dic = [(i, a) for i, a in enumerate(fc)]
        sv = SparseVector(len(fc), dic)
    elif "size" not in fc and "type" in fc and fc["type"] == 1:
        values = fc["values"]
        feature_size = len(values)
        dic = [(i, a) for i, a in enumerate(values)]
        sv = SparseVector(len(values), dic)

    else:
        feature_size = fc["size"]
        sv = Vectors.sparse(fc["size"], list(zip(fc["indices"], fc["values"])))
    return sv

with open(tempOutputLocalPath, "w") as o:
    with open(tempDataLocalPath) as f:
        for line in f.readlines():
            features = parse(line)
            y = model.predict([features])
            o.write(json.dumps({"predict": y.tolist()}) + "\n")

```

和训练是一样，我们依然需要通过mlsql获取输入，输出，以及参数。方式完全一样。在批量预测阶段，
我们需要获得数据地址，模型地址，以及预测结果地址。

```
isp = mlsql.params()["internalSystemParam"]
tempDataLocalPath = isp["tempDataLocalPath"]
tempModelLocalPath = isp["tempModelLocalPath"]
tempOutputLocalPath = isp["tempOutputLocalPath"]
```

现在有了数据，有了模型，以及你最后要写入的地方，你就可以为所欲为了。

> tempDataLocalPath 不是目录，是一个json文件。所以可以直接读取。
> tempOutputLocalPath 也是一个文件名，直接写入。

我们看到有一个parse函数，他主要用来解析json格式的向量是稀疏还是紧凑的，这个在训练阶段我们也用到了。

批量预测结果如下：

```
predict
[0]
[0]
[0]
....
```

> 如果训练时你配置了多个算法或者多组参数 预测时可以在where条件里指定个algIndex。

## API预测

训练后模型，最大的用处就是部署成API服务，通过http暴露出去，这样谁到可以访问。MLSQL提供了将模型转化为函数的机制，这样，该函数可以注册到我们的API服务里。
在前面的训练和批量预测中，我们都是直接在需要时启动Python进程。

但是在API中则不是这样的。当请求到来是，我们会将函数分发到各个已经在运行的python worker中执行。这样可以避免启动的开销，从而达到毫秒级别的延时。

我们来看看，如何将一个模型注册成一个函数：

```sql
register PythonAlg.`/tmp/model` as sk_predict;
```

现在你就有了一个叫做sk_predict的函数了。就是这么简单，对不对？

现在你可以使用sk_predict函数进行预测了：

```sql
select sk_predict(vec_dense(array(5.1,3.5,1.4,0.2))) as predict as output;
```

显示结果如下：

```
predict
{"type":1,"values":[0]}
```

请求耗时大概在100ms左右。


我们看对应的python脚本如何写：

```python
from __future__ import absolute_import
from pyspark.ml.linalg import VectorUDT, Vectors
import pickle
import os
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
    rawVector = pickle.loads(items[0])
    feature = VectorUDT().deserialize(rawVector)
    y = model.predict([feature.toArray()])
    return [VectorUDT().serialize(Vectors.dense(y))]

python_fun.udf(predict)
```

`predict`这个函数名你随意取名字。前面不能改变，必须是index,s。通过如下三行代码将s转化为向量：


```python

items = [i for i in s]
rawVector = pickle.loads(items[0])
feature = VectorUDT().deserialize(rawVector)
```

通过下面的方式加载模型：

```python
modelPath = pickle.loads(items[1])[0] + "/model.pkl"

if not hasattr(os, "mlsql_models"):
    setattr(os, "mlsql_models", {})
if modelPath not in os.mlsql_models:
    print("Load sklearn model %s" % modelPath)
    os.mlsql_models[modelPath] = pickle.load(open(modelPath, "rb"))

model = os.mlsql_models[modelPath]
```

这同样是固定写法。

现在有了模型，有了向量，我们就可以预测了：

```
y = model.predict([feature.toArray()])
return [VectorUDT().serialize(Vectors.dense(y))]
```

预测的结果是数组，则个会根据不同的模型而有所变化，但是最终都要转化为向量，并且返回一个序列化值。

最后，用python_fun将其包装成可序列化的函数。

```python
python_fun.udf(predict)
```


> 大家还记得如何携带资源文件么？比如字典文件什么的？

> 如果训练时你配置了多个算法或者多组参数 预测时可以在where条件里指定个algIndex。







