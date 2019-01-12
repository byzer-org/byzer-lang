# XGBoost

XGBoostExt 基于 [xgboost4j-spark](https://xgboost.readthedocs.io/en/latest/jvm/scaladocs/xgboost4j-spark/index.html).
开发而成，相关参数大家可以参考相应的官方文档。

> 目前只支持spark 2.3.x

XGBoost使用上和其他内置算法完全一样：

```
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

train data1 as XGBoostExt.`/tmp/model`;

```
输出结果：

```

name            value
---------------	------------------
modelPath	/tmp/model/_model_24/model/0
algIndex	0
alg	ml.dmlc.xgboost4j.scala.spark.WowXGBoostClassifier
metrics	
status	true
startTime	20190112 50:18:49:395
endTime	20190112 50:18:54:088
trainParams	Map()
```



## 批量预测

```sql
-- batch predict
predict data1 as XGBoostExt.`/tmp/model`;
```

## API 预测

```sql
-- api predict
register XGBoostExt.`/tmp/model` as npredict;
select npredict(features) from data as output;
```