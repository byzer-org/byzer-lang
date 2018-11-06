### RandomForest

```sql
load libsvm.`/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;
train data as RandomForest.`/tmp/model`;
register RandomForest.`/tmp/model` as predict;
select predict(features)  from data as result;
save overwrite result as json.`/tmp/result`;
```