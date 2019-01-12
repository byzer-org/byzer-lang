# RandomForest

RandomForest是一个分类算法。

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

最后输出结果如下：

```
name   value
---------------------------------
modelPath    /tmp/model/_model_10/model/1
algIndex     1
alg          org.apache.spark.ml.classification.RandomForestClassifier
metrics      f1: 0.7625000000000001 weightedPrecision: 0.8444444444444446 weightedRecall: 0.7999999999999999 accuracy: 0.8
status       success
startTime    20180913 59:15:32:685
endTime      20180913 59:15:36:317
trainParams  Map(maxDepth -> 10)
---------------------------------
modelPath    /tmp/model/_model_10/model/0
algIndex     0
alg          org.apache.spark.ml.classification.RandomForestClassifier
metrics      f1:0.7625000000000001 weightedPrecision: 0.8444444444444446 weightedRecall: 0.7999999999999999 accuracy: 0.8
status       success
startTime    20180913 59:1536:318
endTime      20180913 59:1538:024
trainParams  Map(maxDepth -> 2, featuresCol -> features, labelCol -> label)
```

对于大部分内置算法而言，都支持如下几个特性：

1. 可以通过keepVersion 来设置是否保留版本。
2. 通过fitParam.数字序号 配置多组参数，设置evaluateTable后系统自动算出metrics.


## 批量预测

```
predict data1 as RandomForest.`/tmp/model`;
```

结果如下：

```
features                                label  rawPrediction                                            probability  prediction
{"type":1,"values":[5.1,3.5,1.4,0.2]}	0	{"type":1,"values":[16.28594461094461,3.7140553890553893]}	{"type":1,"values":[0.8142972305472306,0.18570276945276948]}	0
{"type":1,"values":[5.1,3.5,1.4,0.2]}	1	{"type":1,"values":[16.28594461094461,3.7140553890553893]}	{"type":1,"values":[0.8142972305472306,0.18570276945276948]}	0
```

## API预测


```sql
register RandomForest.`/tmp/model` as rf_predict;

-- you can specify which module you want to use:
register RandomForest.`/tmp/model` as rf_predict where
algIndex="0";

-- you can specify which metric the MLSQL should use to get best model
register RandomForest.`/tmp/model` as rf_predict where
autoSelectByMetric="f1";

select rf_predict(features) as predict_label, label from data1 as output;
```

algIndex可以选择我用哪组参数得到的算法。我们也可以让系统自动选择，前提是我们在训练时配置了evalateTable， 这个只要使用autoSelectByMetric即可。
最后，就可以像使用一个函数一样对一个feature进行预测了。
