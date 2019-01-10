# MLSQL

Unify Big Data and Machine Learning

# Example

## Machine Learning

MLSQL is realy easy to use, four lines to train a model and predict.

```sql
load libsvm.`sample_libsvm_data.txt` as data;
train data as RandomForest.`/tmp/model`;
register RandomForest.`/tmp/model` as rf_predict;
select predict(features)  from data as result;
```

## Deep Learning

MLSQL supports deep learning. You also can use any python deep learning library in MLSQL.

```sql
train trainData as BigDLClassifyExt.`/tmp/bigdl` where
disableSparkLog = "true"
and fitParam.0.featureSize="[3,28,28]"
and fitParam.0.classNum="10"
and fitParam.0.maxEpoch="10"
and fitParam.0.code='''
def apply(params:Map[String,String])= {
val model = Sequential()
model.add(Reshape(Array(3, 28, 28), inputShape = Shape(28, 28, 3)))
model.add(Dense(params("classNum").toInt, activation = "softmax").setName("fc2"))
}} '''
```
