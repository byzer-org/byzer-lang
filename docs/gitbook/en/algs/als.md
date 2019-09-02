# ALS

ALS is very popular in collaborative algorithms. It is very convenient to build a recommendation system.
The input data format is relatively simple, requiring userCol, itemCol and ratingCol.

```sql
set jsonStr='''
{"a":1,"i":2,"rate":1},
{"a":1,"i":3,"rate":1},
{"a":2,"i":2,"rate":1},
{"a":2,"i":7,"rate":1},
{"a":1,"i":2,"rate":1},
{"a":1,"i":6,"rate":1},
''';

load jsonStr.`jsonStr` as data;
```

Now you can use ALS for training:

```sql
train data as ALSInPlace.`/tmp/model` where

-- the first group of parameters
`fitParam.0.maxIter`="5"
and `fitParam.0.regParam` = "0.01"
and `fitParam.0.userCol` = "a"
and `fitParam.0.itemCol` = "i"
and `fitParam.0.ratingCol` = "rate"

-- the sencond group of parameters    
and `fitParam.1.maxIter`="1"
and `fitParam.1.regParam` = "0.1"
and `fitParam.1.userCol` = "a"
and `fitParam.1.itemCol` = "i"
and `fitParam.1.ratingCol` = "rate"

-- compute rmse     
and evaluateTable="data"
and ratingCol="rate"

-- size of recommending items for user  
and `userRec` = "10"

-- size of recommending users for item
-- and `itemRec` = "10"
and coldStartStrategy="drop";
```

We configure two sets of parameters and use RMSE to evaluate the effect. We recommend 10 items to each user. If you need to recommend 10 users for each content, set itemRec parameters.

The final results are as follows:

```
name            value
modelPath	    /tmp/model/_model_14/model/0
algIndex	    0
alg	            org.apache.spark.ml.recommendation.ALS
metrics	        rmse: -0.011728744197876936
status	        success
startTime	    20190112 19:18:59:826
endTime	        20190112 20:18:02:280
trainParams	    Map(ratingCol -> rate, itemCol -> i, userCol -> a, regParam -> 0.01, maxIter -> 5)
........
```

Storage and results:

```sql
load parquet.`/tmp/model/data/userRec` as userRec;
select * from userRec as result;
```



```
a   recommendations
1	[{"i":2,"rating":0.9975529},{"i":3,"rating":0.9835032},{"i":6,"rating":0.9835032},{"i":7,"rating":0.805835}]
2	[{"i":2,"rating":0.9970016},{"i":7,"rating":0.9838716},{"i":3,"rating":0.82125527},{"i":6,"rating":0.82125527}]
```

## prediction algorithm declare

This algorithm does not support batch prediction and API prediction.