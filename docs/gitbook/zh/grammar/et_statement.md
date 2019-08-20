# Train/Run/Predict语法

将这三者放在一起，是因为他们有完全一致的语法。他们的存在是为了弥补Select对机器学习以及自定义数据处理支持的不够好的短板。

## 基础语法

首先是train。 train顾名思义，就是进行训练，主要是对算法进行训练时使用。下面是一个比较典型的示例：

```sql

load json.`/tmp/train` as trainData;

train trainData as RandomForest.`/tmp/rf` where
keepVersion="true"
and fitParam.0.featuresCol="content"
and fitParam.0.labelCol="label"
and fitParam.0.maxDepth="4"
and fitParam.0.checkpointInterval="100"
and fitParam.0.numTrees="4"
;
```

如果我们用白话文去读这段话，应该是这么念的：

```
加载位于/tmp/train目录下的，数据格式为json的数据，我们把这些数据叫做trainData, 接着，
我们对trainData进行训练，使用算法RandomForest，将模型保存在/tmp/rf下，训练的参数为 fitParam.0.* 指定的那些。
```

其中fitParam.0 表示第一组参数，用户可以递增设置N组，MLSQL会自动运行多组，最后返回结果列表。

run/predict 具有完全一致的用法，但是目的不同。 run的语义是对数据进行处理，而不是训练，他是符合大数据处理的语义的。我们下面来看一个例子：

```sql
run testData as RepartitionExt.`` where partitionNum="5" as newdata; 
```

格式和train是一致的，那这句话怎么读呢？

```
运行testData数据集，并且使用RepartitionExt进行处理，处理的参数是partitionNum="5"，最后处理后的结果我们取名叫newdata
```
RepartitionExt是用于从新分区的一个模块，也就是将数据集重新分成N分，N由partitionNum配置。

那么predict呢？ predict语句我们一看，应该就知道是和机器学习相关的，对的，他是为了批量预测用的。比如前面，我们将训练随机森林的结果模型放在了
`/tmp/rf` 目录下，现在我们可以通过predict语句加载他，并且预测新的数据。

```sql
predict testData as RandomForest.`/tmp/rf`;
```

这句话的意思是，对testData进行预测，预测的算法是RandomForest，对应的模型在/tmp/rf下。

通过上面的举例，我们知道，train/predict是应用机器学习领域的，分别对应建模和预测。run主要是数据处理用的。在前面提及的RandomForest，RepartitionExt，
我们叫做ET,用户都可以实现自己ET，添加新的功能特性。MLSQL提供了良好的扩展机制方便用户进行自己的封装，可以说，整个MLSQL大部分功能集合都是基于
ET做扩展而形成的。

