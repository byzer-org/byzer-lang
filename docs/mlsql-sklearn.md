## MLSQL-SKLearn

前提条件：
拥有一个可以访问的Kafka(1.0 版本以上),并且允许自动创建topic。

MLSQL 支持SKlearn算法。具体使用方法如下：

```sql
load libsvm.`/Users/allwefantasy/Softwares/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;

train data as SKLearn.`/tmp/model`  
where `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and `kafkaParam.topic`="test"
and `kafkaParam.group_id`="g_test-1"
and  `fitParam.0.batchSize`="1000"
and  `fitParam.0.labelSize`="2"
and  `fitParam.0.alg`="MultinomialNB"
and `systemParam.pythonPath`="python";
and `systemParam.pythonVer`="2.7";
;
register SKLearn.`/tmp/model` as nb_predict;
select vec_array(nb_predict(features))  as k from data
```

其中，因为采用了SKlearn里的partial_fit,所以可以通过设置fitParam.0.batchSize 表示每批次给算法数据量。

fitParam.0.labelSize 告诉分类数目
systemParam.pythonPath 和 systemParam.pythonVer 分别设置executor节点python的路径和版本。kafkaParam 则是配置一个kafka实例。

MLSQL训练时会fork一个python进行进行训练，预测时会fork一个python demon进程，然后python deamon进行启动python worker 去处理预测逻辑。

目前支持的算法有：

1. GradientBoostingClassifier
2. RandomForestClassifier
3. SVC
4. MultinomialNB

