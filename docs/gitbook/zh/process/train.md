# train语法

前面我们说过 train语法和run是完全一致的。一般train用于做算法训练，比如下面的例子：

```sql
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

-- specify the test dataset which will be used to feed evaluator to generate some metrics e.g. F1, Accurate
and evaluateTable="data1"

-- specify group 0 parameters
and `fitParam.0.featuresCol`="features"
and `fitParam.0.featuresCol`="label"
and `fitParam.0.maxDepth`="2"

-- specify group 1 parameters
and `fitParam.1.featuresCol`="features"
and `fitParam.1.labelCol`="label"
and `fitParam.1.maxDepth`="10";
```

train语法一般后面可以不用接表名，如果接了也是可以的，该表返回的是训练的状态，比如是不是成功等。实际训练好的算法模型
其实是放在了/tmp/model下面。

这是上面脚本返回的状态：

```
name                             value
------------------------------------------------------------------
model                           Path/tmp/model/_model_0/model/1
algIndex                        1
alg                             org.apache.spark.ml.classification.RandomForestClassifier
metrics                         f1: 0.7625000000000001 weightedPrecision: 0.8444444444444446 weightedRecall: 0.7999999999999999 accuracy: 0.8
status                          success
startTime                       20190111 26:12:55:971
endTime                         20190111 26:12:59:233
trainParams                     Map(labelCol -> label, featuresCol -> features, maxDepth -> 10)
------------------------------------------------------------------
modelPath                       /tmp/model/_model_0/model/0
algIndex                        0
alg                             org.apache.spark.ml.classification.RandomForestClassifier
metrics
status                          fail
startTime                       20190111 26:12:59:235
endTime                         20190111 26:12:59:251
trainParams                     Map(maxDepth -> 2, featuresCol -> label, labelCol -> features)
```

在这示例中，我们通过fitParam.0 /gitParam.1 配置了两组参数，这样系统会把两组参数都跑出来，并且默认使用f1确定那个会更好。
其中第二组成功，第一组失败，通过 status字段可以看得到。那为什么失败呢，日志给出详细问题：

```
[2019-01-11 12:29:47,942][INFO ][org.apache.spark.scheduler.DAGScheduler] Job 64 finished: countByValue at MulticlassMetrics.scala:42, took 0.023014 s
[2019-01-11 12:29:47,943][INFO ][streaming.dsl.mmlib.algs.SQLRandomForest] [owner] [admin] [trained] [alg=org.apache.spark.ml.classification.RandomForestClassifier] [metrics=List(MetricValue(f1,0.7625000000000001), MetricValue(weightedPrecision,0.8444444444444446), MetricValue(weightedRecall,0.7999999999999999), MetricValue(accuracy,0.8))] [model hyperparameters=cacheNodeIds: If false, the algorithm will pass trees to executors to match instances with nodes. If true, the algorithm will cache node IDs for each instance. Caching can speed up training of deeper trees. (default: false)	checkpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext (default: 10)	featureSubsetStrategy: The number of features to consider for splits at each tree node. Supported options: auto, all, onethird, sqrt, log2, (0.0-1.0], [1-n]. (default: auto)	featuresCol: features column name (default: features, current: features)	impurity: Criterion used for information gain calculation (case-insensitive). Supported options: entropy, gini (default: gini)	labelCol: label column name (default: label, current: label)	maxBins: Max number of bins for discretizing continuous features.  Must be >=2 and >= number of categories for any categorical feature. (default: 32)	maxDepth: Maximum depth of the tree. (>= 0) E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. (default: 5, current: 10)	maxMemoryInMB: Maximum memory in MB allocated to histogram aggregation. (default: 256)	minInfoGain: Minimum information gain for a split to be considered at a tree node. (default: 0.0)	minInstancesPerNode: Minimum number of instances each child must have after split.  If a split causes the left or right child to have fewer than minInstancesPerNode, the split will be discarded as invalid. Should be >= 1. (default: 1)	numTrees: Number of trees to train (>= 1) (default: 20)	predictionCol: prediction column name (default: prediction)	probabilityCol: Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities (default: probability)	rawPredictionCol: raw prediction (a.k.a. confidence) column name (default: rawPrediction)	seed: random seed (default: 207336481)	subsamplingRate: Fraction of the training data used for learning each decision tree, in range (0, 1]. (default: 1.0)	thresholds: Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold (undefined)]
[2019-01-11 12:29:47,943][INFO ][streaming.dsl.mmlib.algs.SQLRandomForest] [owner] [admin] [training] [alg=org.apache.spark.ml.classification.RandomForestClassifier] [keepVersion=true]
[2019-01-11 12:29:47,957][INFO ][streaming.dsl.mmlib.algs.SQLRandomForest] [owner] [admin] java.lang.IllegalArgumentException: requirement failed: Column label must be of type struct<type:tinyint,size:int,indices:array<int>,values:array<double>> but was actually double.
```

原因是我们第一组参数配置错误，把 featuresCol配置成了 "label"字段。 正如前面所说，你可以通过load语法获取所有可配置参数：

```
load modelParams.`RandomForest` as output;
```

更多算法细节我们会在后续机器学习篇章中介绍。