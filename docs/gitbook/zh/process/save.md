## Save语法

在前面的文章中，我们已经多次看到了save语法的身影。 save和load是对应的，load是加载数据映射成表，save则是把表存储到具体的存储引擎里。
比如

```sql
set rawData=''' 
{"jack":1,"jack2":2}
{"jack":2,"jack2":3}
''';

set savePath="/tmp/jack";

load jsonStr.`rawData` as table1;

save overwrite table1 as json.`${savePath}`;

```

save 后面一个参数overwrite|append可选。表示覆盖或者追加。 save语法最后可以接where条件。比如：

```sql
save overwrite table1 as json.`${savePath}` where fileNum="10";
```

这样，存储的内容会被分成10份，通过这种方式可以避免大量的小文件。

对于表，我们其实也可以直接通过run语法来先进行分区：

```sql
run table1 as RepartitionExt.`` where partitionNum="10" as newTable;
save overwrite newTable as json.`/tmp/newTable`;

```
在前面数据源章节，我们也演示了如何使用save将数据存储到各种存储引擎里，比如es/mysql等。