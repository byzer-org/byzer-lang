# 数据集切分

在做算法时，我们需要经常对数据切分成train/test。但是如果有些分类数据特别少，可能就会都跑到一个集合里去了。
RateSample 支持对每个分类的数据按比例切分。

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
```

现在我们使用RateSample进行切分：

```sql
train data as RateSampler.`/tmp/model` 
where labelCol="label"
and sampleRate="0.7,0.3";

load parquet.`/tmp/model` as output ;
```

其中labelCol指定按什么字段进行切分，sampleRate指定切分比例。

结果如下：

```
features            label   __split__
[5.1,3.5,1.4,0.2]	1	    0
[5.1,3.5,1.4,0.2]	1	    0
[4.7,3.2,1.3,0.2]	1	    1
```

数据集多出了一个字段 __split__, 0 表示0.7那个集合（训练集）， 1表示0.3那个集合（测试集）。

可以这么使用

```sql
select * from output where __split__=1
as validateTable;

select * from output where __split__=0
as trainingTable;
```

默认RateSampler采用估算算法，如果数据集较小，可以通过参数isSplitWithSubLabel="true" 获得非常精确的划分。


## 如何在预测时使用

该模块无法在预测中使用。


