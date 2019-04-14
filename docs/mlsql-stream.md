StreamingPro现在也支持用XQL定义流式计算了

## Stream XQL

当你根据[编译文档](https://github.com/allwefantasy/streamingpro/blob/master/docs/compile.md) 编译以及运行StreamingPro Service后，
你就可以通过一些http接口提交流式任务了。

通过接口 http://ip:port/run/script

post 参数名称为：sql

下面脚本是个自定义数据源的例子输出console的例子，具体脚本内容为：

```
```sql
-- mock some data.
set data='''
{"key":"yes","value":"no","topic":"test","partition":0,"offset":0,"timestamp":"2008-01-24 18:01:01.001","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":1,"timestamp":"2008-01-24 18:01:01.002","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":2,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":3,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":4,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":5,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
''';

-- load data as table
load jsonStr.`data` as datasource;

-- convert table as stream source
load mockStream.`datasource` options
stepSizeRange="0-3"
as newkafkatable1;

-- aggregation
select cast(key as string) as k,count(*) as c  from newkafkatable1 group by key
as table21;

-- output the the result to console.
save append table21
as console.``
options mode="Complete"
and duration="10"
and checkpointLocation="/tmp/cpl3";

```

提交该任务后，日志可以看到输出：
```
-------------------------------------------
Batch: 6
-------------------------------------------
+-------+-------+-------+--------------------+
|column1|column2|column3|          kafkaValue|
+-------+-------+-------+--------------------+
|      m|      m|      m|[yes, 0, 5, 2008-...|
+-------+-------+-------+--------------------+
```

你可以以kafka为数据源，脚本如下：

```sql

-- if you are using kafka 0.10
load kafka.`pi-content-realtime-db` options
`kafka.bootstrap.servers`="---"
and `subscribe`="---"
as kafka_post_parquet;

-- if you are using kafka 0.8.0/0.9.0
load kafka8.`pi-content-realtime-db` options
`kafka.bootstrap.servers`="---"
and `topics`="---"
as kafka_post_parquet;

```

你可以将结果数据写入mysql

```sql
-- connect mysql as the data sink.
connect jdbc where
driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/wow"
and driver="com.mysql.jdbc.Driver"
and user="---"
and password="----"
as mysql1;
-- Save the data to MYSQL
save append table21
as streamJDBC.`mysql1.test1`
options mode="append"
and `driver-statement-0`="create table test1 if not exists........."
-- executed in executor
and `statement-0`="replace into wow.test1(k,c) values(?,?)"
-- one batch duration
and duration="3"
and checkpointLocation="/tmp/cpl3";
```

你还可以将结果写入parquet静态分区表或者动态分区表

静态分区脚本：

```sql

save append table
as streamParquet.`/table/hp_stat_date=${pathDate.toString("yyyy-MM-dd")}`
options mode="Append"
and duration="30"
and checkpointLocation="/tmp/ckl1";
```

动态分区脚本：

```sql

save append table
as streamParquet.`table`
options mode="Append"
and duration="10"
and checkpointLocation="/tmp/ckl1"
partitionBy partition_field;
```

mode 有三种模式：

Append模式：顾名思义，既然是Append，那就意味着它每次都是添加新的行，那么也就是说：它适用且只适用于那些一旦产生计算结果便永远不会去修改的情形， 所以它能保证每一行数据只被数据一次

Complete模式：整张结果表在每次触发时都会全量输出！这显然是是要支撑那些针对数据全集进行的计算，例如：聚合

Update模式：某种意义上是和Append模式针锋相对的一个种模式，它只输出上次trigger之后，发生了“更新”的数据的，这包含新生的数据和行

你可以用window聚合，并使用watermark处理延时数据，脚本如下：

```sql

select ts,f1 as table1;

-- register watermark for table1
register WaterMarkInPlace.`table1` as tmp1
options eventTimeCol="ts"
and delayThreshold="10 seconds";

-- process table1
select f1,count(*) as num from table1
-- 30 minutes为窗口大小，10 seconds为滑动时间
group by f1, window(ts,"30 minutes","10 seconds")
as table2;

save append table2 as ....
```

通过接口：

http://ip:port/stream/jobs/running
查看所有流式计算任务。

通过接口：

http://ip:port/stream/jobs/kill?groupId=....
可以杀死正在运行的流式任务

当然，你也可以打开SparkUI查看相关信息。
