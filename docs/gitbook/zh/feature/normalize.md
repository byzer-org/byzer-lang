# 特征归一化

特征归一化，在MLSQL对应的ET为NormalizeInPlace。本质上，该方式是为了统一量纲，让一个向量里的元素变得可以比较。
这对于比如KMeans,nearest neighbors methods, RBF kernels, 以及任何依赖于距离的算法，都是必要的。

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

接着我们对第一列数据a,b两列数据按照行的方式进行归一化。

```sql
train data as NormalizeInPlace.`/tmp/model`
where inputCols="a,b"
and scaleMethod="standard"
and removeOutlierValue="false"
;

load parquet.`/tmp/model/data` 
as output;
```

结果如下：

```
a                   b   label
-0.5069956180959223	0	0
-0.2802902604107538	0	1
1.7806675367271416	0	0
-0.48638604012454334	0	0
-0.5069956180959223	0	1
```

removeOutlierValue设置为true，会自动用中位数填充异常值。


>如果inputCols只有一列，那么该列可以为double数组 


## 如何在预测时使用

任何ET都具备在"训练时学习到经验"转化为一个函数，从而可以使得你把这个功能部署到流式计算，API服务里去。同时，部分ET还有batch predict功能，
可以让在批处理做更高效的预测。

对于ET NormalizeInPlace 而言，我们要把它转化为一个函数非常容易：

```sql

register NormalizeInPlace.`/tmp/model` as convert;

```

通过上面的命令，NormalizeInPlace就会把训练阶段学习到的东西应用起来，现在，任意给定两个数字，都可以使用`convert`函数将
内容转化为向量了。

```sql
select convert(array(7,8)) as features as output;
```

输出结果为：

```
features
[-0.4932558994483363,0]
```

