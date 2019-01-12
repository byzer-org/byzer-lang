# 部署流程

我们对MLSQL的Local模式做了优化，从而实现毫秒级的预测效果。


1. 只要以local模式启动MLSQL，通常你可以认为这是一个标准的Java应用：

```
./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[2] \
--name predict_service \
streamingpro-mlsql-x.x.xjar    \
-streaming.name predict_service    \
-streaming.platform spark   \
-streaming.rest true   \
-streaming.driver.port 9003   \
-streaming.spark.service true \
-streaming.thrift false \
-streaming.enableHiveSupport true \
-streaming.deploy.rest.api true 
```

其中最后一行`-streaming.deploy.rest.api true`开启了优化，可以让MLSQL示例跑的更快。


2. 访问 `http://127.0.0.1:9003/run/script` 接口动态注册在训练阶段生成的模型：

```sql
register TfIdfInPlace.`/tmp/tfidf_model` as tfidf_predict;
register RandomForest.`/tmp/rf_model` as bayes_predict;
```


3. 访问：`http://127.0.0.1:9003/model/predict`进行预测请求： 

请求参数为：

```sql
dataType=row
data=[{"feature":[1,2,3...]}]
sql=select bayes_predict(vec_dense(feature)) as p
```

| Property Name	 | Default  |Meaning |
|:-----------|:------------|:------------|
|dataType|vector|data字段的数据类型，目前只支持vector/string/row|
|data|[]|你可以传递一个或者多个vector/string/json,必须符合json规范|
|sql|None|用sql的方式调用模型，其中如果是vector/string,则模型的参数feature是固定的字符串，如果是row则根据key决定|
|pipeline|None|用pipeline的方式调用模型，写模型名，然后逗号分隔，通常只能支持vector/string模式|


### 完整例子

启动MLSQL集群，进行训练：

```sql
--NaiveBayes
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

-- use RandomForest
train data1 as RandomForest.`/tmp/model` where

-- once set true,every time you run this script, MLSQL will generate new directory for you model
keepVersion="true" 

-- specicy the test dataset which will be used to feed evaluator to generate some metrics e.g. F1, Accurate
and evaluateTable="data1"

-- specify group 0 parameters
and `fitParam.0.labelCol`="features"
and `fitParam.0.featuresCol`="label"
and `fitParam.0.maxDepth`="2"

-- specify group 1 parameters
and `fitParam.1.featuresCol`="features"
and `fitParam.1.labelCol`="label"
and `fitParam.1.maxDepth`="10"
;

```

按前面启动一个MLSQL java程序，然后注册前面的模型：

```
register RandomForest.`/tmp/model` as rf_predict;
```

接着就可以外部调用API使用了,需要传递两个参数：

```
dataType=row
data=[{"feature":[1,2,3...]}]
sql=select bayes_predict(vec_dense(feature)) as p
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

另外，大部分模块都是可以通过这种方式进行注册，不仅仅是算法。这就实现了端到端的部署，允许你将预处理逻辑也部署上。
通过sql select 语法，你还可以完成一些较为复杂的预测逻辑。