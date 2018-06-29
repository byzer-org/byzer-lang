## 用户自定义算法（Python）

MLSQL 提供了一个叫PythonAlg的模块，允许用户使用Python算法框架（比如SKlearn,Tensorflow,Keras等等）自定义训练和预测过程。

前置条件：

1. 需要一个没有权限校验，并且可以自动创建主题的Kafka
2. MLSQL需要对/tmp/\_\_mlsql\_\_ 目录有完全的权限。
3. 每个服务器都需要拥有python相关的环境，比如pyspark以及常见标准库。如果你需要sklearn,则需要在每台服务器上都安装Sklearn。

更多示例可查看项目：[mlsql](https://github.com/allwefantasy/mlsql/tree/master/examples)

### 使用范例

```sql
-- 加载一个已经向量化好的数据。你也可以使用其他预处理模块
-- data表已经包含了features字段和label字段
load libsvm.`sample_libsvm_data.txt` as data;

--
train data as PythonAlg.`/tmp/pa_model`
where

-- 用户自定义的python训练脚本,需要符合一定的规范。
pythonScriptPath="${pythonScriptPath}"
-- kafka 配置
and `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and `kafkaParam.topic`="test"
and `kafkaParam.group_id`="g_test-1"

-- 如果开启，则不通过kafka进行中转，而是将数据写入到hdfs然后分发到各个executor节点上
and  enableDataLocal="true"
and  dataLocalFormat="json"

-- 一些配置参数
and  `fitParam.0.batchSize`="1000"
and  `fitParam.0.labelSize`="2"

-- 验证数据集
and validateTable="data"

and `systemParam.pythonPath`="python"
and `systemParam.pythonVer`="2.7"
;

-- 把模型注册成一个函数，并且指定具体逻辑。
register PythonAlg.`/tmp/pa_model` as jack options
pythonScriptPath="${pythonPredictScriptPath}"
;

-- 使用该函数进行数据预测
select jack(features) from data
as newdata;
```

从示例代码可以看到，用户需要提供两个脚本，一个是训练脚本，一个是预测脚本。我这里会以sklearn为例子。


首先是训练脚本,你需要把streamingpro-spark-2.0里resource/python 目录下的mlsql.py,mlsql_model.py,python_fun.py,msg_queue.py 
四个文件拷贝到你的项目里。

```python

import os
import json
from pyspark.ml.linalg import Vectors
from sklearn.naive_bayes import MultinomialNB
import pickle

# 如果想脱离MLSQL测试和使用，那么可以直接拼凑参数。
# 正式使用时记得要把这些代码给删除，否则会导致在mlsql里配置不生效。
PARAM_FILE = "python_temp.pickle"
# if True and not os.path.exists(PARAM_FILE):
with open(PARAM_FILE, "wb") as f:
    pickle.dump({"fitParam": {},
                 "internalSystemParam": {
                     "tempDataLocalPath": "/tmp/william/tmp/pa_model/tmp/data/",
                     "tempModelLocalPath": "/tmp/william/tmp/pa_model2"
                 },
                 "systemParam": {}
                 }, f)

import mlsql_model
import mlsql


# 使用SKlearn贝叶斯模型
clf = MultinomialNB()

'''
mlsql.sklearn_configure_params 会把配置参数都设置到clf中。
除了自动配置，大家也可以通过mlsql.params()拿到所有的配置选项。
''' 

mlsql.sklearn_configure_params(clf)

# 如果开启了enableDataLocal，则可以通过这个方式拿到tempDataLocalPath，也就是数据目录
# 因为前面配置的dataLocalFormat是json,所以这里面存储的是json文件格式数据
tempDataLocalPath = mlsql.internal_system_param["tempDataLocalPath"]

print(tempDataLocalPath)

## 解析json格式数据，要求输入的都是向量，但是可以存储成不同的格式，比如json,csv
files = [file for file in os.listdir(tempDataLocalPath) if file.endswith(".json")]
res = []
res_label = []
for file in files:
    with open(tempDataLocalPath + "/" + file) as f:
        for line in f.readlines():
            obj = json.loads(line)
            f_size = obj["features"]["size"]
            f_indices = obj["features"]["indices"]
            f_values = obj["features"]["values"]
            res.append(Vectors.sparse(f_size, f_indices, f_values).toArray())
            res_label.append(obj["label"])


def train(X, y, label_size):
    clf.partial_fit(X, y, classes=range(label_size))

## 训练
train(res,res_label,2)

## 获取校验集
X_test, y_test = mlsql.get_validate_data()

if len(X_test) > 0:
    testset_score = clf.score(X_test, y_test)
    print("mlsql_validation_score:%f" % testset_score)

## 保存模型
'''
模型保存的地方是需要通过配置获取的，比如这里的sk_save_model方法获取模型地址的方式如下：
 
  isp = mlsql.params()["internalSystemParam"]
  tempModelLocalPath = isp["tempModelLocalPath"] if "tempModelLocalPath" in isp else "/tmp/"
这样系统才能拿到你训练好的模型并且分发到其他节点。
'''
mlsql_model.sk_save_model(clf)

```

其中，mlsql_model,mlsql 是MLSQL提供的一些辅助工具。
为了便于使用，你可以直接从项目里 streamingpro-spark-2.0 的resource 文件夹的python子目录里的所有python文件拷贝到你的项目里，从而
方便代码提示以及测试。

在上面的示例代码中，我已经提供了注释。

如果我们使用Kafka作为数据传输的话(也就是把enableDataLocal设置为false)，那么获取数据只需要通过一个指令：

```sql
rd = mlsql.read_data()
for items in rd(max_records=batch_size):
    X = [item[x_name].toArray() for item in items]
    y = [item[y_name] for item in items]
    [do what you want]
```

算法训练完成后，我们需要能够进行预测，用户也是可以定义这个预测方式的，下面是一个示例脚本：

```python

from pyspark.ml.linalg import VectorUDT, Vectors
import pickle
import os
import python_fun


# 定义一个预测函数，签名是固定的，index表示分区，s表示数据。
# s 表示一条预测数据，是一个数组，长度为2。
# 第一个元素是一个vector,你需要通过pickle反序列化后再转化为vector表示。
# 第二个元素是模型在本地的位置，模型文件名则由你自己决定。
# 我这里加载的是Sklearn的模型。但是我们需要保证预测的速度，所以不应该每次都加载模型,
# 应该保持模型加载的单例。
def predict(index, s):
    items = [i for i in s]
    
    # pickle.loads(items[1])[0]  表示的是modelPath,也就是前面配置的/tmp/william/tmp/pa_model2
    modelPath = pickle.loads(items[1])[0]+"/model.pickle"
    print("predict.....")
    
    ## 用一个比较trick的方法解决模型只加载一次的问题
    if not hasattr(os,"models"):
         setattr(os,"models",{})
       if modelPath not in os.models:
           print("load model.....")
           os.models[modelPath] = pickle.load(open(modelPath))
    
    model = os.models[modelPath]
    
    # items[0] 就是一个向量，需要通过VectorUDT进行反序列化
    vector = pickle.loads(items[0])
    feature = VectorUDT().deserialize(vector)
    
        
    y = model.predict([feature.toArray()])
    return [VectorUDT().serialize(Vectors.dense(y))]


# 对该函数进行序列化
python_fun.udf(predict)

```

这里的python_fun也是MLSQL提供的一个工具类。
在当前阶段，我们做了一个约定，模型应该是vector in vector out的，所以输入和输出是固定的，内部逻辑则由你决定。

写好这个脚本后，就可以注册模型为函数了：

```
-- 把模型注册成一个函数，并且指定具体逻辑。
register PythonAlg.`/tmp/pa_model` as jack options
pythonScriptPath="${pythonPredictScriptPath}"
;
```

预测代码可以完全在本地进行测试，核心就是构造predict 里的`s`变量。
 
```python
if __name__ == '__main__':
    # 把模型和数据下载到本地，假设数据是json格式的
    model_path = '/tmp/model'
    data_path = 'tmp/part-00000-94b58e22-3671-460e-a7d2-120469c94057-c000.json'

    with open(file=data_path) as f:
        for line in f.readlines():
            s = []
            feature = json.loads(line)['feature']['values'] 
            # 对特征字段要先进行VectorUDT序列化，然后再用picle进行序列化
            s.insert(0, pickle.dumps(VectorUDT().serialize(Vectors.dense(feature))))
            # modelPath 直接用pickle序列化就好
            s.insert(1, pickle.dumps([model_path]))
            # 调用前面的方法进行预测
            predict(1, s)
```

## 模型目录结构

```
/tmp/william/tmp/pa_model
├── meta
│   ├── 0
│   │   ├── _SUCCESS
│   │   └── part-00000-4bfa8d3e-cb51-41a1-a996-54d8f5ffc672-c000.snappy.parquet
│   ├── 1
│   │   ├── _SUCCESS
│   │   └── part-00000-764afc64-33be-4029-a5ea-b56b9d7cbbc4-c000.snappy.parquet
│   └── params
│       ├── _SUCCESS
│       ├── part-00000-14c18428-823a-4041-a9c6-0e0587485e10-c000.snappy.parquet
│       └── part-00001-14c18428-823a-4041-a9c6-0e0587485e10-c000.snappy.parquet
├── model
│   └── 0
│       └── model.pickle
└── tmp
    └── data
        ├── _SUCCESS
        └── part-00000-ddffe5a3-70b9-4886-b5a0-c824ba1e6784-c000.json
```

目录结构。 

1. meta 下有三个目录， 0存储模型元数据，比如path路径等，1存储一些环境信息，比如python版本等。params则存储所有训练参数。
2. model目录下以0为序号，存储模型的二进制表达。如果是多个模型，则以1递增。
3. tmp/data 则是开了本地数据后，训练数据会存储在该目录。

这些数据存储在HDFS上，根据需要会分发到各个executor节点的/tmp/\_\_mlsql\_\_ 目录。


## 参数说明

### fitParam.[number].resource.[resourceName]

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
