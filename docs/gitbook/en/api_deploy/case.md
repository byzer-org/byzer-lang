# Deployment process

We optimized the Local mode of MLSQL to achieve millisecond prediction effect.

1.Starting MLSQL in local mode can be considered a standard Java application

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

Turn on parameter optimization `-streaming.deploy.rest.api true` to make the MLSQL example run faster.

2.Accessing the `http://127.0.0.1:9003/run/script` interface can dynamically register the models generated during the training phase:


```sql
register TfIdfInPlace.`/tmp/tfidf_model` as tfidf_predict;
register RandomForest.`/tmp/rf_model` as bayes_predict;
```


3.Visit `http://127.0.0.1:9003/model/predict` for prediction requests.

The request parameters are as follows

```sql
dataType=row
data=[{"feature":[1,2,3...]}]
sql=select bayes_predict(vec_dense(feature)) as p
```

| Property Name	 | Default  |Meaning |
|:-----------|:------------|:------------|
|dataType|vector|Currently only vector/string/row is supported|
|data|[]|You can pass one or more vector/string/json parameters, which must conform to the JSON specification|
|sql|None|用sql的方式调用模型，其中如果是vector/string,则模型的参数feature是固定的字符串，如果是row则根据key决定|
|pipeline|None|用pipeline的方式调用模型，写模型名，然后逗号分隔，通常只能支持vector/string模式|


### Complete examples

Start MLSQL cluster training

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

According to the method described earlier, start a MLSQL Java program and register training model.
```
register RandomForest.`/tmp/model` as rf_predict;
```

After passing two parameters, API can be called externally for use

```
dataType=row
data=[{"feature":[1,2,3...]}]
sql=select bayes_predict(vec_dense(feature)) as p
```

The final prediction results are as follows

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

In addition, most modules except algorithm can be registered in this way. This implements end-to-end deployment, allowing pre-processing logic to be deployed as well.Through SQL SELECT grammar, you can also complete some more complex prediction logic.