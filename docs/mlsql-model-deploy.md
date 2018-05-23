## MLSQL 模型部署


### 概览
使用MLSQL完成模型训练后，这个时候，我们肯定想迫不及待的把模型部署然后提供API服务。
通常，模型使用的场景有三个：

1. 批处理。    比如对历史数据做统一做一次预测处理。
2. 流式计算。  希望把模型部署在流式程序里。
3. API服务。   希望通过API 对外提供模型预测服务。（这是一种最常见的形态）
 

通过MLSQL算法得到的模型，都会被注册成UDF函数使用，所以对于批处理和流式，天然就能够做到很好的支持。

为了方便把模型部署成API服务，MLSQL也提供了必要的支持。


### 部署方式

1. 只要以local模式启动StreamingPro后，通常你可以认为这是一个标准的Java应用：

```
./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[2] \
--name predict_service \
streamingpro-spark-2.0-1.0.0.jar    \
-streaming.name predict_service    \
-streaming.job.file.path file:///tmp/query.json \
-streaming.platform spark   \
-streaming.rest true   \
-streaming.driver.port 9003   \
-streaming.spark.service true \
-streaming.thrift false \
-streaming.enableHiveSupport true
```

2. 访问 `http://127.0.0.1:9003/run/script` 接口动态注册已经生成的模型：

```sql
register NaiveBayes.`/tmp/bayes_model` as bayes_predict;
```


3. 访问：`http://127.0.0.1:9003/model/predict`进行预测请求： 

请求参数为：

```sql
dataType=vector
data=[[1,2,3...]]
sql=select bayes_predict(feature) as p
```

| Property Name	 | Default  |Meaning |
|:-----------|:------------|:------------|
|dataType|vector|data字段的数据类型，目前只支持vector/string|
|data|[]|你可以传递一个或者多个vector/string,必须符合json规范|
|sql|None|用sql的方式调用模型，其中模型的参数feature是固定的字符串|
|pipeline|None|用pipeline的方式调用模型，写模型名，然后逗号分隔|


典型的比如TfIdfInPlace模型接受的参数就是字符串，这个时候dataType就要设置成string。

如果你不愿意使用类似sql的语法做预测，也可以将sql参数替换为pipeline参数，如下：

```sql
dataType=string
data=["你好"，"大家好"]
pipeline=tfidf,bayes_predict
```

这里会使用tfidf模型先对数据进行处理，然后接着把tfidf处理的结果给bayes_predict。
本质上是一个嵌套调用，如下：

```
bayes_predict(tfidf(feature))
```

### 完整例子

启动一个训练的StreamingPro Service，提交如下脚本：

```sql
--NaiveBayes
load libsvm.`/Users/allwefantasy/Softwares/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;
train data as NaiveBayes.`/tmp/bayes_model`;
register NaiveBayes.`/tmp/bayes_model` as bayes_predict;
select bayes_predict(features)  from data as result;
save overwrite result as json.`/tmp/result`;

```

启动一个预测StreamingPro API Server,先注册模型：

```
register NaiveBayes.`/tmp/bayes_model` as bayes_predict;
```

接着就可以外部调用API使用了,需要传递两个参数：

```

data=[[1,2,3...]]
sql=select bayes_predict(feature) as p
```

最后的预测结果为：

```
{
    "p": {
        "type": 1,
        "values": [
            1,
            0
        ]
    }
}

```

当前版本data只支持多个向量，不支持张量。后续会支持传递shape。向量支持dense 和 sparse两种模式。如果是sparse 模式，则需要额外
传递参数vecSize

### 部署不是MLSQL生成的SKLearn模型

你也可以自己生成一个SKLearn模型，不过模型保存成文件的序列化机制必须是pickle。注册时，你需要添加几个额外的参数：

```
register NaiveBayes.`/tmp/bayes_model` as bayes_predict
options nonMLSQLModel="true"
and pythonPath="python"
and pythonVer="2.7"
;
```
nonMLSQLModel 告诉系统这不是一个MLSQL生成的模型文件，另外，你还需要通过pythonPath，pythonVer 参数告知系统这个模型需要的python版本。


