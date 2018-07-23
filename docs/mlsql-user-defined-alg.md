

- [**自定义Python算法**](#用户自定义算法（Python）)
  - [使用范例](#使用范例)
  - [如果我训练或者预测需要传递多个脚本文件怎么办？](#如果我训练或者预测需要传递多个脚本文件怎么办？)
  - [模型版本管理](#模型版本管理)
  - [模型目录结构](#模型目录结构)
  - [资源文件配置](#资源文件配置) 
  


### 用户自定义算法（Python）

MLSQL 提供了一个叫PythonAlg的模块，允许用户使用Python算法框架（比如SKlearn,Tensorflow,Keras等等）自定义训练和预测过程。

前置条件：

1. 需要一个没有权限校验，并且可以自动创建主题的Kafka
2. MLSQL需要对/tmp/\_\_mlsql\_\_ 目录有完全的权限。
3. 每个服务器都需要拥有python相关的环境，比如pyspark以及常见标准库。如果你需要sklearn,则需要在每台服务器上都安装Sklearn。

我们有一个专门的示例项目，里面有包含图片，自然语言处理如何整合到PythonAlg模块：[mlsql-python](https://github.com/allwefantasy/mlsql/tree/master/examples)



### 使用范例

假设我们要训练一个模型，是这么用的：

```sql
-- train sklearn model
train data as PythonAlg.`${modelPath}` 

-- specify the location of the training script 
where pythonScriptPath="${sklearnTrainPath}"

-- kafka params for log
and `kafkaParam.bootstrap.servers`="${kafkaDomain}"
and `kafkaParam.topic`="test"
and `kafkaParam.group_id`="g_test-2"
and `kafkaParam.userName`="pi-algo"
-- distribute training data, so the python training script can read 
and  enableDataLocal="true"
and  dataLocalFormat="json"

-- sklearn params
-- use SVC
and `fitParam.0.moduleName`="sklearn.svm"
and `fitParam.0.className`="SVC"
and `fitParam.0.featureCol`="features"
and `fitParam.0.labelCol`="label"
and `fitParam.0.class_weight`="balanced"
and `fitParam.0.verbose`="true"

-- and `fitParam.1.moduleName`="sklearn.naive_bayes"
-- and `fitParam.1.className`="GaussianNB"
-- and `fitParam.1.featureCol`="features"
-- and `fitParam.1.labelCol`="label"
-- and `fitParam.1.class_weight`="balanced"
-- 贝叶斯会采用partial_fit,所以需要事先告诉分类数目
-- and `fitParam.1.labelSize`="26"

-- python env
and `systemParam.pythonPath`="python"
and `systemParam.pythonParam`="-u"
and `systemParam.pythonVer`="2.7";
```

从上面的代码来看，我们需要提供一个python脚本完成训练。

首先是训练脚本,你需要把streamingpro-mlsql里resource/python 目录下的mlsql.py,mlsql_model.py,python_fun.py,msg_queue.py 
四个文件拷贝到你的项目里。

这个训练脚本的大致流程是这样的：

```python

## 导入mlsql模块
import mlsql

## 定义一个简单的方法获取从 PythonAlg配置的参数
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

batchSize = int(param("batchSize", "64"))
labelSize = int(param("labelSize", "-1"))
## 通过mlsql模块获取数据所在的目录
tempDataLocalPath = mlsql.internal_system_param["tempDataLocalPath"]
## 进行训练
model.fit(tempDataLocalPath)

## 获得模型需要存储的路径
if "tempModelLocalPath" not in mlsql.internal_system_param:
    raise Exception("tempModelLocalPath is not configured")

tempModelLocalPath = mlsql.internal_system_param["tempModelLocalPath"]

if not os.path.exists(tempModelLocalPath):
    os.makedirs(tempModelLocalPath)

model_file_path = tempModelLocalPath + "/model.pkl"
print("Save model to %s" % model_file_path)
## 将模型存储在系统告知的路径，当然，比如tensorflow 会有自己的导出方式
pickle.dump(model, open(model_file_path, "wb"))

```

简单来说，就是

1. 获取参数
2. 获取数据目录
3. 训练
4. 把模型保存到指定目录

完毕。

下面是一个非常完整的Sklearn的例子：

```python

from __future__ import absolute_import
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

run_for_test = False
## 如果脱离MLSQL环境，你需要自己构建一些参数，这个就是为了方便这个脚本本机也能运行
if run_for_test:
    from sklearn.naive_bayes import GaussianNB

    # config parameters
    PARAM_FILE = "python_temp.pickle"
    pp = {'internalSystemParam': {'tempModelLocalPath': '/tmp/__mlsql__/3242514c-4113-4105-bdc5-9987b28f9764/0',
                                  'tempDataLocalPath': '/Users/allwefantasy/Downloads/data1', 'stopFlagNum': 9},
          'systemParam': {'pythonVer': '2.7', 'pythonPath': 'python'},
          'fitParam': {'labelCol': 'label', 'featureCol': 'features', 'height': '100', 'width': '100',
                       'modelPath': '/tmp/pa_model', 'labelSize': '2', 'class_weight': '{"1":2}',
                       'moduleName': 'sklearn.svm',
                       'className': 'SVC'},
          'kafkaParam': {'topic': 'zhuhl_1528712229620', 'bootstrap.servers': '127.0.0.1:9092',
                         'group_id': 'zhuhl_test_0', 'userName': 'zhuhl', 'reuse': 'true'}}

    with open(PARAM_FILE, "wb") as f:
        pickle.dump(pp, f)

    # test data
    VALIDATE_FILE = "validate_table.pickle"
    # 1, 100,100,4
    with open(VALIDATE_FILE, "wb") as f:
        data = np.random.random((10, 100, 100, 3))
        pickle.dump([pickle.dumps({"feature": i, "label": [0, 0, 0, 1]}) for i in data.tolist()], f)

## 导入mlsql模块
import mlsql

## 定义一个简单的方法获取从 PythonAlg配置的参数
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

batchSize = int(param("batchSize", "64"))
labelSize = int(param("labelSize", "-1"))

## 加载数据，转化为稀疏矩阵
def load_sparse_data():
    import mlsql
    ## 通过mlsql模块获取数据所在的目录
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

## 正常加载数据
def load_batch_data():
    import mlsql
    ## 通过mlsql模块获取数据所在的目录
    tempDataLocalPath = mlsql.internal_system_param["tempDataLocalPath"]
    datafiles = [file for file in os.listdir(tempDataLocalPath) if file.endswith(".json")]
    X = []
    y = []
    count = 0
    for file in datafiles:
        with open(tempDataLocalPath + "/" + file) as f:
            for line in f.readlines():
                obj = json.loads(line)
                fc = obj[featureCol]
                if "size" not in fc and "type" not in fc:
                    dic = [(i, a) for i, a in enumerate(fc)]
                    sv = SparseVector(len(fc), dic)
                elif "size" not in fc and "type" in fc and fc["type"] == 1:
                    values = fc["values"]
                    dic = [(i, a) for i, a in enumerate(values)]
                    sv = SparseVector(len(values), dic)
                else:
                    sv = Vectors.sparse(fc["size"], list(zip(fc["indices"], fc["values"])))
                count += 1
                X.append(sv.toArray())
                if type(obj[labelCol]) is list:
                    y.append(np.array(obj[labelCol]).argmax())
                else:
                    y.append(obj[labelCol])
                if count % batchSize == 0:
                    yield X, y
                    X = []
                    y = []

## 动态创建Sklearn算法
def create_alg(module_name, class_name):
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    return class_()

## 配置SKlearn参数
def configure_alg_params(clf):
    def class_weight(value):
        if value == "balanced":
            clf.class_weight = value
        else:
            clf.class_weight = dict([(int(k), int(v)) for (k, v) in json.loads(value).items()])

    def max_depth(value):
        clf.max_depth = int(value)

    options = {
        "class_weight": class_weight,
        "max_depth": max_depth
    }

    def t(v, convert_v):
        if type(v) == float:
            return float(convert_v)
        elif type(v) == int:
            return int(convert_v)
        elif type(v) == list:
            json.loads(convert_v)
        elif type(v) == dict:
            json.loads(convert_v)
        elif type(v) == bool:
            return bool(convert_v)
        else:
            return convert_v

    for name in clf.get_params():
        if name in mlsql.fit_param:
            if name in options:
                options[name](mlsql.fit_param[name])
            else:
                dv = clf.get_params()[name]
                setattr(clf, name, t(dv, mlsql.fit_param[name]))


model = create_alg(moduleName, className)
configure_alg_params(model)

## 如果支持partial_fit 则无需使用稀疏矩阵
if not hasattr(model, "partial_fit"):
    X, y = load_sparse_data()
    model.fit(X, y)
else:
    assert labelSize != -1
    print("using partial_fit to batch_train:")
    batch_count = 0
    for X, y in load_batch_data():
        model.partial_fit(X, y, [i for i in xrange(labelSize)])
        batch_count += 1
        print("partial_fit iteration: %s, batch_size:%s" % (str(batch_count), str(batchSize)))

## 获得模型需要存储的路径
if "tempModelLocalPath" not in mlsql.internal_system_param:
    raise Exception("tempModelLocalPath is not configured")

tempModelLocalPath = mlsql.internal_system_param["tempModelLocalPath"]

if not os.path.exists(tempModelLocalPath):
    os.makedirs(tempModelLocalPath)

model_file_path = tempModelLocalPath + "/model.pkl"
print("Save model to %s" % model_file_path)
## 将模型存储在系统告知的路径
pickle.dump(model, open(model_file_path, "wb"))

```


如果我们使用Kafka作为数据传输的话(也就是把enableDataLocal设置为false)，那么获取数据只需要通过一个指令：

```sql
rd = mlsql.read_data()
for items in rd(max_records=batch_size):
    X = [item[x_name].toArray() for item in items]
    y = [item[y_name] for item in items]
    [do what you want]
```

算法训练完成后，我们需要能够进行预测，用户也是可以定义这个预测方式的，下面是一个示例脚本：


```sql
-- specify the location of python predict script 
register PythonAlg.`${modelPath}` as predict options 
pythonScriptPath="${sklearnPredictPath}"
-- we also can specify which model to use
-- and algIndex="0"
;


select predict(features) as predict_label, label from validate_data 
as validate_data;
```



具体的Python预测脚本如下：

```python

from __future__ import absolute_import
from pyspark.ml.linalg import VectorUDT, Vectors
import pickle
import os

run_for_test = False
if run_for_test:
    import mlsql.python_fun
else:
    import python_fun


def predict(index, s):
    ## 大家照着写就好了
    items = [i for i in s]
    modelPath = pickle.loads(items[1])[0] + "/model.pkl"

    if not hasattr(os, "mlsql_models"):
        setattr(os, "mlsql_models", {})
    if modelPath not in os.mlsql_models:
        print("Load sklearn model %s" % modelPath)
        os.mlsql_models[modelPath] = pickle.load(open(modelPath, "rb"))

    model = os.mlsql_models[modelPath]
    ## 获取到预测的向量
    rawVector = pickle.loads(items[0])
    ## 我们需要反序列化
    feature = VectorUDT().deserialize(rawVector)
    ## 转化为python数组后进行预测
    y = model.predict([feature.toArray()])
    ## 把结果转化为vector然后再序列化返回
    return [VectorUDT().serialize(Vectors.dense(y))]

## 方便本地测试
if run_for_test:
    import json

    model_path = '/tmp/__mlsql__/3242514c-4113-4105-bdc5-9987b28f9764/0'
    data_path = '/Users/allwefantasy/Downloads/data1/part-00000-03769d42-1948-499b-8d8f-4914562bcfc8-c000.json'

    with open(file=data_path) as f:
        for line in f.readlines():
            s = []
            wow = json.loads(line)['features']
            feature = Vectors.sparse(wow["size"], list(zip(wow["indices"], wow["values"])))
            s.insert(0, pickle.dumps(VectorUDT().serialize(feature)))
            s.insert(1, pickle.dumps([model_path]))
            print(VectorUDT().deserialize(predict(1, s)[0]))

python_fun.udf(predict)


```

## 如果我训练或者预测需要传递多个脚本文件怎么办？

很多算法同学会把训练和预测方法都写在一个脚本文件里。那么在预测的时候，又要拷贝一份，这回比较繁琐。
有的时候，无论训练或者预测，都需要多个python文件。在MLSQL里对此的总体解决思路是，把非入口python script
作为资源传递，然后通过importlib 动态记在想要的模块。

在MLSQL里，资源文件需要在训练的时候配置，在预测的时候也可以拿到训练时配置的资源文件。

训练的脚本配置：

```sql
set trainScriptPath="/semantic_match/AttenLSTM_mlsql_train_noEmb.py";
set resourceAPath="/semantic_match/AttenLSTM_mlsql_resource_noEmb.py";


train zhuml_corpus_featurize_training as PythonAlg.`${modelPath}` 
where pythonScriptPath="${trainScriptPath}"
-- kafka params for log
and `kafkaParam.bootstrap.servers`="${kafkaDomain}"
and `kafkaParam.topic`="test"
and `kafkaParam.group_id`="g_test-2"

-- distribute data
and  enableDataLocal="true"
and  dataLocalFormat="json"

-- here specify AttenLSTM_mlsql_resource_noEmb.py as resource python script
------------------------------------------------
and `fitParam.0.resource.b`="${resourceAPath}"
------------------------------------------------

and `fitParam.0.train_sample_weight`="1.9691380349608198,4.10711591651999,4.0211718365337275"
and `fitParam.0.labelCol`="label"
and `fitParam.0.queryCol`="query"
and `fitParam.0.candidateCol`="candidate"
and `fitParam.0.epoches`="20"
and `fitParam.0.batch_size`="100"
and `fitParam.0.hidden_unit`="100"
and `fitParam.0.embedding_dim`="100"
and `fitParam.0.early_stopping_round`="1000"
and `fitParam.0.max_seq_len`="100"
and `fitParam.0.num_class`="3"
and `fitParam.0.optimizer`="RMSprop"
and `fitParam.0.loss_function`="mean_squared_error"

-- python env
and `systemParam.pythonPath`="/usr/local/anaconda3/bin/python"
and `systemParam.pythonVer`="3.6";

```

预测时候可以这样使用

```python
from __future__ import absolute_import
from pyspark.ml.linalg import VectorUDT, Vectors
import pickle
import os
import json
import numpy as np
import mlsql
import python_fun
import importlib
import importlib.machinery
import mlsql

## get resource file path
model_module_path = mlsql.internal_system_param['resource']['b']


def predict(index, sample):
    # use importlib to load resource python script dynamically
    model_module = importlib.machinery.SourceFileLoader("AttenLSTM_mlsql_resource_noEmb", model_module_path)
    mm = model_module.load_module()
    # use module method to predict
    return mm.predict(1, sample)

python_fun.udf(predict)

```


### 模型版本管理

PythonAlg模块支持版本管理。只需在训练时开启

```sql
keepVersion="true"
```

开启后，

1. 每次训练都会新生成一个子目录
2. 注册的时候会自动使用最新的版本
3. 可以手动指定版本
4. 可以查看每个版本的状态


目录接口如下(假设你设置的目录是/tmp/william/pa_model_k):

```
/tmp/william/pa_model_k/_model_[version_number]
```

如果想看这个模型成功失败等详细信息：

```
load parquet.`/tmp/william/pa_model_k/_model_n/meta/0` as modelInfo;
select * from modelInfo as result;
```

手动指定版本的方式如下：

```sql
register PythonAlg.`/tmp/william/pa_model_k` as predict options
modelVersion="1" ;
```

对应的会加载

```
/tmp/william/pa_model_k/_model_1 
```

这个目录的信息




### 模型目录结构

```
/tmp/william/tmp/pa_model_k

|_____model_0
| |____meta
| | |____0
| | |____1
| |____model
| | |____0
|____tmp
| |____data
```

_model_0 表示第一个版本的模型，内部包含的目录有：

1. meta 下有2个目录， 0存储模型元数据，比如path路径,失败成功，1存储一些环境信息，比如python版本等，训练参数等
2. model目录下以0为序号，存储模型的二进制表达。


tmp/data 则是开了本地数据后，训练数据会存储在该目录。

这些数据存储在HDFS上，根据需要会分发到各个executor节点的/tmp/\_\_mlsql\_\_ 目录中。


### 资源文件配置


```
fitParam.[number].resource.[resourceName]
```

`PythonAlg`模块指定特殊的数据源，比如我们可能在算法中需要到一个字典文件。我们可以通过`resource`配置来指定相应的资源文件，系统会将自动分发到运行的节点上。

```sql
-- 一些配置参数
and  `fitParam.0.batchSize`="1000"
and  `fitParam.0.labelSize`="2"
and `fitParam.0.resource.a`="/path/to/resource/a"
and `fitParam.0.resource.b`="/path/to/resource/b"
```

> NOTE: `resource`配置支持多个资源文件路径

配置完成之后，我们可以在python脚本中这样使用：

```python
print(mlsql.internal_system_param['resource']['a'])
print(mlsql.internal_system_param['resource']['b'])
```

如果在训练参数重配置了资源文件，那么系统将自动在预测时候加载该资源文件，使用方式同训练方法。
