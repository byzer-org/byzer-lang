# 特征平滑

ScalerInPlace 支持min-max,log2,logn 去平滑数据。不同于后续章节会介绍的NormalizeInPlace，该ET针对的是列。

我们先构造一些测试数据：

```sql
-- create test data
set jsonStr='''
{"a":1,    "b":100, "label":0.0},
{"a":100,  "b":100, "label":1.0}
{"a":1000, "b":100, "label":0.0}
{"a":10,   "b":100, "label":0.0}
{"a":1,    "b":100, "label":1.0}
''';
load jsonStr.`jsonStr` as data;

```

接着我们对第一列数据a,b两列数据都进行平滑。

```sql
train data as ScalerInPlace.`/tmp/scaler`
where inputCols="a,b"
and scaleMethod="min-max"
and removeOutlierValue="false"
;

load parquet.`/tmp/scaler/data` 
as featurize_table;
```

结果如下：

```
a                    b   label
0	                 0.5	0
0.0990990990990991	 0.5	1
1	                 0.5	0
0.009009009009009009 0.5	0
0	                 0.5	1
```

removeOutlierValue设置为true，会自动用中位数填充异常值。


## 如何在预测时使用

任何ET都具备在"训练时学习到经验"转化为一个函数，从而可以使得你把这个功能部署到流式计算，API服务里去。同时，部分ET还有batch predict功能，
可以让在批处理做更高效的预测。

对于ET ScalerInPlace 而言，我们要把它转化为一个函数非常容易：

```sql

register ScalerInPlace.`/tmp/scaler` as scale_convert;

```

通过上面的命令，ScalerInPlace就会把训练阶段学习到的东西应用起来，现在，任意给定两个数字，都可以使用`scale_convert`函数将
内容转化为向量了。

```sql
select scale_convert(array(7,8)) as features as output;
```

输出结果为：

```
features
[0.006006006006006006,0.5]
```

