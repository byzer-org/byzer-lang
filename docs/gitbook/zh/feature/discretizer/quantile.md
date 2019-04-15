# Quantile

 Quantile使用更加简单，你只要指指定bucket数目即可。
 
 假设我们有如下数据：
 

```sql
-- create test data
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
select features[0] as a ,features[1] as b from data
as data1;
```

现在我们得到了a,b两个字段，我们对他们分别进行切分，转化为离散值：

```sql
train data1 as Discretizer.`/tmp/model`
where method="quantile"
and `fitParam.0.inputCol`="a"
and `fitParam.0.outputCol`="a_v"
and `fitParam.0.numBuckets`="3"
and `fitParam.1.inputCol`="b"
and `fitParam.1.outputCol`="b_v"
and `fitParam.1.numBuckets`="3";

```


这里，我们使用fitParam.0 表是第一组切分规则，fitParam.1表示第二组切分规则。fitParam.0 负责对a切分，fitParam.1负责对b切分。

> 该ET目前比较特殊查看切分结果需要使用register语法注册函数。
> 需要注意的是，spark 2.4.x 要求outputCol必须设置。所以我们设置下。

参数描述：

|parameter|default|comments|
|:----|:----|:----|
|method|bucketizer|support: bucketizer, quantile|
|fitParam.${index}.inputCols|None|double类型字段|
|fitParam.${index}.splitArray|None|bucket array，-inf ~ inf ，size should > 3，[x, y)|

## 如何在预测时使用

任何ET都具备在"训练时学习到经验"转化为一个函数，从而可以使得你把这个功能部署到流式计算，API服务里去。同时，部分ET还有batch predict功能，
可以让在批处理做更高效的预测。

对于ET NormalizeInPlace 而言，我们要把它转化为一个函数非常容易：

```sql

register Discretizer.`/tmp/model` as convert;

```

通过上面的命令，Discretizer 就会把训练阶段学习到的东西应用起来，现在，可以使用`convert`函数了。

```sql
select convert(array(7,8)) as features as output;
```

输出结果为：

```
features
[1,1]
```

都被分在2分区里。



 