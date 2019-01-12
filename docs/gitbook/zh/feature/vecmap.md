# Map转化为向量

VecMapInPlace 可以把一个 Map[String,Double] 转化为一个向量。具体使用案例：

假设我们有如下数据：

```sql
set jsonStr='''
{"features":{"a":1.6,"b":1.2},"label":0.0}
{"features":{"a":1.5,"b":0.2},"label":0.0}
{"features":{"a":1.6,"b":1.2},"label":0.0}
{"features":{"a":1.6,"b":7.2},"label":0.0}
''';
load jsonStr.`jsonStr` as data;

register ScriptUDF.`` as convert_st_to_map where
code='''
def apply(row:org.apache.spark.sql.Row) = {
  Map("a"->row.getAs[Double]("a"),"b"->row.getAs[Double]("b"))
}
''';

select convert_st_to_map(features) as f from data as newdata;
```

这里我们使用了自定义UDF去将Row转化为Map，用户方便后续的示例。大家可以参看 `创建UDF/UDAF` 相关章节的内容。

接着我们就可以进行学习训练了：

```sql
train newdata as VecMapInPlace.`/tmp/model`
where inputCol="f";

load VecMapInPlace.`/tmp/model/data` as output;
```

显示结果如下：

```
f
{"type":0,"size":2,"indices":[0,1],"values":[1.6,1.2]}
{"type":0,"size":2,"indices":[0,1],"values":[1.5,0.2]}
{"type":0,"size":2,"indices":[0,1],"values":[1.6,1.2]}
{"type":0,"size":2,"indices":[0,1],"values":[1.6,7.2]}
```
可以看到已经转化为一个二维向量了。

## 如何在预测时使用

任何ET都具备在"训练时学习到经验"转化为一个函数，从而可以使得你把这个功能部署到流式计算，API服务里去。同时，部分ET还有batch predict功能，
可以让在批处理做更高效的预测。

对于ET NormalizeInPlace 而言，我们要把它转化为一个函数非常容易：

```sql

register VecMapInPlace.`/tmp/model` as convert;

```

通过上面的命令，VecMapInPlace 就会把训练阶段学习到的东西应用起来，现在，可以使用`convert`函数了。

```sql
select convert(map("a",1,"b",0)) as features as output;
```

输出结果为：

```
features
{"type":0,"size":2,"indices":[0,1],"values":[1,0]}
```

通常，我们需要对向量做平滑或者归一化，请参考相应章节。



