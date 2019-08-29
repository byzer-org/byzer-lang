# Register语法

在前面章节，我们已经介绍过train/predict语法。register语法其实和他们一样，也是为了配合机器学习而设置的。我们看下面的列表说明：

1. train ,对数据进行训练从而得到模型。
2. predict, 使用已经得到的模型，对数据进行批量预测。
3. register, 将模型注册成UDF函数，方便在批，流,API中使用。

从上面我们看到，register 主要目的是将模型注册成UDF函数。随着MLSQL的发展，
他还具备将Python/Scala/Java代码动态转化为SQL函数的能力，以及在流式计算力也有自己特定的作用。

## 注册模型

如果我们需要将一个已经训练好的模型注册成一个函数，那时相当简单的：

```sql
load json.`/tmp/train` as trainData;

train trainData as RandomForest.`/tmp/rf` where
....
;

register  RandomForest.`/tmp/rf` as rf_predict;

select rf_predict(features) as predict_label from trainData
as output;
```

现在我们的单独把register拿出来：

```sql
register  RandomForest.`/tmp/rf` as rf_predict;
```

怎么念呢？

```
将 RandomForest模型/tmp/rf 注册成一个函数，函数名叫rf_predict
```

之后，我们就可以在后续的部分使用这个函数了。


## 注册函数

在SQL中，最强大的莫过于函数了。在Hive中，其相比其他的传统数据而言，在于其可以很好的进行函数的扩展。 MLSQL将这个优势发展到极致。我们来看看如何新建
一个函数：

```sql
register ScriptUDF.`` as plusFun where
and lang="scala"
and udfType="udf"
code='''
def apply(a:Double,b:Double)={
   a + b
}
''';
```

使用ET ScriptUDF注册一个函数叫plusFun,这个函数使用scala语言，函数的类型是UDF,对应的实现代码在code参数里。

接着，我们就可以使用这个函数进行数据处理了：

```sql
 -- create a data table.
 set data='''
 {"a":1}
 {"a":1}
 {"a":1}
 {"a":1}
 ''';
 load jsonStr.`data` as dataTable;
 
 -- using echoFun in SQL.
 select plusFun(1,2) as res from dataTable as output;
``` 

是不是很简单。除了Scala以外，我们还支持scala/python/java等语言。

## 流式计算中的使用

在流式计算中，有wartermark以及window的概念。MLSQL的register语法也可以用于流式计算里的watermark的注册。

```sql
-- register watermark for table1
register WaterMarkInPlace.`table1` as tmp1
options eventTimeCol="ts"
and delayThreshold="10 seconds";
```

这里大家只要有个感觉就行。本章节我们不会做过多解释，在后续专门的流式计算章节，我们会提供非常详细的说明。

