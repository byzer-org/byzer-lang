# LDA

主题模型是我非常喜欢的一个模型。为什么呢？ 首先它是无监督，其次他可以给词和内容都可以算出N个主题的概率分布，从而使得词和内容都可以计算了。


下面看看如何使用：

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

train data1 as LDA.`/tmp/model` where

-- k: number of topics, or number of clustering centers
k="3"

-- docConcentration: the hyperparameter (Dirichlet distribution parameter) of article distribution must be >1.0. The larger the value is, the smoother the predicted distribution is
and docConcentration="3.0"

-- topictemperature: the hyperparameter (Dirichlet distribution parameter) of the theme distribution must be >1.0. The larger the value is, the more smooth the distribution can be inferred
and topicConcentration="3.0"

-- maxIterations: number of iterations, which need to be fully iterated, at least 20 times or more
and maxIter="100"

-- setSeed: random seed
and seed="10"

-- checkpointInterval: interval of checkpoints during iteration calculation
and checkpointInterval="10"

-- optimizer: optimized calculation method currently supports "em" and "online". Em method takes up more memory, and multiple iterations of memory may not be enough to throw a stack exception
and optimizer="online"
;
```

上面大部分参数都不需要配置。训练完成后会返回状态：

```
---------------	------------------
modelPath	/tmp/model/_model_21/model/0
algIndex	0
alg	org.apache.spark.ml.clustering.LDA
metrics	
status	success
startTime	20190112 36:18:02:057
endTime	20190112 36:18:06:873
trainParams	Map()
```

## 批量预测

```sql
predict data1 as LDA.`/tmp/model` ;
```

结果如下：

```
features                                label                              topicDistribution
{"type":1,"values":[5.1,3.5,1.4,0.2]}	0	                               {"type":1,"values":[0.9724967882100011,0.01374292627483604,0.01376028551516305]}
```

目前批量预测还不支持词的主题分布计算。

## API预测

> 目前只支持spark 2.3.x

```
register LDA.`/tmp/model` as lda;
select label,lda(4) topicsMatrix,lda_doc(features) TopicDistribution,lda_topic(label,4) describeTopics from data as result;
```

同样的当你注册lda函数事，会给隐式生成多个函数:

1. lda        接受一个词
2. lda_doc    接受一个文档
3. lda_topic  接受一个主题，以及显示多少词



