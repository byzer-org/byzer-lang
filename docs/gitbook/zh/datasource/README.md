#数据源

MLSQL使用load语法进行数据源的加载，加载完成后，可以通过表名进行应用和查询。
本质上，MLSQL是通过load语法把数据源映射成一张表，然后就可以用标准的SQL或者其他方式进行处理了。

测试中，我们经常会自己制造一些数据，可以像这么用：

```sql
set rawData=''' 
 {"a":1,"b":2}
 {"a":1,"b":3}
''';

load jsonStr.`rawData` as data;

```

首先我们通过set语法设置了一个变量，这个变量可以使用三个`'` 来包括大段的文本，包括换行。
接着我们通过jsonStr数据源来加载这个普通的文本成为一张表，这个表我们取名为data.

如果正想描述load这个句子，就是 以jsonStr为数据源，加载字符变量 rawData,加载后的结果是一张表，并且我们取名为
data.

接着我们就可以这样使用了：

```
select * from data as newdata;
```

其中as语句后的newdata也是一张表明，代表的是经过select后得到的一张新表。

除了jsonStr,我们还能以相同的方式加载ElasticSearch,JDBC(如MySQL)等。后续章节我们会详细介绍。

