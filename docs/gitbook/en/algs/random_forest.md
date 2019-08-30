# RandomForest

RandomForest is a classification algorithm.

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

The results are as follows:

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

For most built-in algorithms, the following features are supported:

1.you can set the version by keepVersion parameter.

2.The system calculates metrics automatically through fitParam and evaluateTable parameters.



## batch prediction

```
predict data1 as RandomForest.`/tmp/model`;
```

the result as follows：

```
features                                label  rawPrediction                                            probability  prediction
{"type":1,"values":[5.1,3.5,1.4,0.2]}	0	{"type":1,"values":[16.28594461094461,3.7140553890553893]}	{"type":1,"values":[0.8142972305472306,0.18570276945276948]}	0
{"type":1,"values":[5.1,3.5,1.4,0.2]}	1	{"type":1,"values":[16.28594461094461,3.7140553890553893]}	{"type":1,"values":[0.8142972305472306,0.18570276945276948]}	0
```

## API prediction


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