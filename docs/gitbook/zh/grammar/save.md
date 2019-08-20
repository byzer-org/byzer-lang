# Save语法

save语法类似传统SQL中的insert语法。insert其实在MLSQL中也能使用，不过我们强烈建议使用save语法来取代。在前面章节里，
我们介绍了load语法，有数据的加载，必然就有数据的存储，存储也需要支持无数的数据源，还会涉及到Schema的问题。

## 基本语法

我们先来看一个基本的例子：

```sql
set rawData=''' 
{"jack":1,"jack2":2}
{"jack":2,"jack2":3}
''';

set savePath="/tmp/jack";

load jsonStr.`rawData` as table1;


```

最后一句单拎出来：

```sql
save overwrite table1 as json.`${savePath}`;
```

怎么读呢？ 将table1覆盖保存，使用json格式，保存位置为 ${savePath}。 还是比较易于理解的吧。通常，save语句里的
数据源或者格式和load是保持一致的，配置参数也类似。但有部分数据源只支持save,有部分只支持load。典型的比如 jsonStr,他就
只支持load,不支持save。

如果我想讲数据分成十份保存呢？ save也支持where条件子句：

```sql
save overwrite table1 as json.`${savePath}` where fileNum="10";
```

另外Save支持四种存储方式：

1. overwrite
2. append
3. ignore
4. errorIfExists

还记得我们前面介绍的Connect语句么，connect语句主要是简化连接一些复杂的数据源，需要配置特别多的参数的问题。这个在save语句中也是同样适用的。

比如下面的例子：

```sql
set user="root";

connect jdbc where
url="jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
and driver="com.mysql.jdbc.Driver"
and user="${user}"
and password="${password}"
as db_1;

save append tmp_article_table as jdbc.`db_1.crawler_table`;
```

你也可以完全讲connect里面的参数放到save的where子句中。值得注意的是，MLSQL的connect语句的生命周期是application级别。意味着你只要运行一次，
就全生命周期有效。



