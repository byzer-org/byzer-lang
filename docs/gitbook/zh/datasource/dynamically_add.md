# 运行时添加数据源依赖

我们启动MLSQL-Engine之后，会发现，哦，忘了添加MongoDB依赖了。这个时候无法通过MLSQL语言访问MongoDB。
MLSQL支持数据源中心的概念，可以动态添加需要的依赖。

过滤出mongodb数据源：

```sql
select 1 as a as fakeTable;
set repository="http://datasource.repository.mlsql.tech";

run fakeTable as DataSourceExt.`` where repository="${repository}" and command="list" as datasource;
select * from datasource where name="mongo" as output;
```

在结果里，我们会看到对于MongoDB而言有两个jar包依赖，一个是spark驱动，一个是原生的driver驱动。
右侧里面罗列了所有可选版本。

接着，我们分别根据前面的信息，把两个jar包都加上：

```sql
run fakeTable as DataSourceExt.`mongo/org.mongodb/mongo-java-driver/3.9.0` where repository="${repository}" and command="add";
run fakeTable as DataSourceExt.`mongo/org.mongodb.spark/mongo-spark-connector_2.11/2.4.0` where repository="${repository}" and command="add";
```

这个时候，相应的jar包会被添加到driver/executor端，接着你就可以操作MongoDB了，比如：

```sql
set data='''
{"jack":"cool"}
''';

load jsonStr.`data` as data1;

save overwrite data1 as mongo.`twitter/cool` where
    partitioner="MongoPaginateBySizePartitioner"
and uri="mongodb://127.0.0.1:27017/twitter";

load mongo.`twitter/cool` where
    partitioner="MongoPaginateBySizePartitioner"
and uri="mongodb://127.0.0.1:27017/twitter"
as table1;
select * from table1 as output1;
```
 

