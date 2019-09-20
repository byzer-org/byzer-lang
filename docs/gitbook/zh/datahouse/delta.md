#Delta加载和存储以及流式支持

Delta本质就是HDFS上一个目录。这就意味着你可以在自己的主目录里欢快的玩耍。我们会分如下几个部分介绍Delta的使用：

1. 基本使用
2. 按数据库表模式使用
3. Upsert语义的支持
4. 流式更新支持 
5. 小文件合并
6. 同时加载多版本数据为一个表
7. 查看表的状态(如文件数等)

## 基本使用

```sql
set rawText='''
{"id":1,"content":"MLSQL是一个好的语言","label":0.0},
{"id":2,"content":"Spark是一个好的语言","label":1.0}
{"id":3,"content":"MLSQL语言","label":0.0}
{"id":4,"content":"MLSQL是一个好的语言","label":0.0}
{"id":5,"content":"MLSQL是一个好的语言","label":1.0}
{"id":6,"content":"MLSQL是一个好的语言","label":0.0}
{"id":7,"content":"MLSQL是一个好的语言","label":0.0}
{"id":8,"content":"MLSQL是一个好的语言","label":1.0}
{"id":9,"content":"Spark好的语言","label":0.0}
{"id":10,"content":"MLSQL是一个好的语言","label":0.0}
''';

load jsonStr.`rawText` as orginal_text_corpus;

save append orginal_text_corpus as delta.`/tmp/delta/table10`;
```

执行上面的语句，我们就能成功的将相关数据写入delta数据湖了。

加载的方式如下：

```sql
load delta.`/tmp/delta/table10` as output;
```

## 按数据库表模式使用

很多用户并不希望使用路径，他们希望能够像使用hive那样使用数据湖。MLSQL对此也提供了支持。在Engine启动时，加上参数

```
-streaming.datalake.path /tmp/datahouse
```

系统便会按数据湖模式运行。此时用户不能自己写路径了，而是需要按db.table的模式使用。


加载数据湖表：

```sql
load delta.`public.table1` as table1;
```

保存数据库湖表：

```sql
save append table1  
as delta.`public.table1`
partitionBy col1;
```

如果你开启了权限验证 ，并且使用MLSQL Console时，那么需要按如下方式配置权限：

![](http://docs.mlsql.tech/upload_images/WX20190831-113125@2x.png)

![](http://docs.mlsql.tech/upload_images/WX20190831-113136@2x.png)

如果是流式，delta换成rate.从权限上看，我们依然是按路径进行授权的。但是在MLSQL中，用户看到的是库和表。

你可以使用下列命令查看所有的数据库和表：

```sql
!delta show tables;
```

## Upsert语义的支持

Delta支持数据的Upsert操作，对应的语义为： 如果存在则更新，不存在则新增。

我们前面保存了十条数据，现在尝试如下代码：

```sql
set rawText='''
{"id":1,"content":"我更新了这条数据","label":0.0}
''';

load jsonStr.`rawText` as orginal_text_corpus;

save append orginal_text_corpus  
as delta.`/tmp/delta/table10` 
and idCols="id";
```
我们看到id为1的数据已经被更新为。

![](http://docs.mlsql.tech/upload_images/WX20190819-192447.png)


## 流式更新支持

这里，我们会简单涉及到流式程序的编写。大家可以先有个感觉，不用太关注细节。我们后续专门的流式章节会提供更详细的解释和说明。

为了完成这个例子，用户可能需要在本机启动一个Kafka,并且版本是0.10.0以上。

首先，我们通过MLSQL写入一些数据到Kafka:

```sql
set abc='''
{ "x": 100, "y": 201, "z": 204 ,"dataType":"A group"}
''';
load jsonStr.`abc` as table1;

select to_json(struct(*)) as value from table1 as table2;
save append table2 as kafka.`wow` where 
kafka.bootstrap.servers="127.0.0.1:9092";
```

接着启动一个流式程序消费：

```sql
-- the stream name, should be uniq.
set streamName="kafkaStreamExample";

!kafkaTool registerSchema 2 records from "127.0.0.1:9092" wow;

-- convert table as stream source
load kafka.`wow` options 
kafka.bootstrap.servers="127.0.0.1:9092"
and failOnDataLoss="false"
as newkafkatable1;

-- aggregation 
select *  from newkafkatable1
as table21;

-- output the the result to console.
save append table21  
as rate.`/tmp/delta/wow-0` 
options mode="Append"
and idCols="x,y"
and duration="5"
and checkpointLocation="/tmp/s-cpl6";
```

这里，我们指定x,y为联合主键。

现在可以查看了：

```sql
load delta.`/tmp/delta/wow-0` as show_table1;
select * from show_table1 where x=100 and z=204 as output;
```

## 小文件合并

MLSQL对Delta的小文件合并的要求比较苛刻，要求必须是append模式，不能发生更新已有记录的的表才能进行小文件合并。

我们在示例中模拟一些Kafka的数据：

```sql
set data='''
{"key":"a","value":"no","topic":"test","partition":0,"offset":0,"timestamp":"2008-01-24 18:01:01.001","timestampType":0}
{"key":"a","value":"no","topic":"test","partition":0,"offset":1,"timestamp":"2008-01-24 18:01:01.002","timestampType":0}
{"key":"b","value":"no","topic":"test","partition":0,"offset":2,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"b","value":"no","topic":"test","partition":0,"offset":3,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"b","value":"no","topic":"test","partition":0,"offset":4,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"b","value":"no","topic":"test","partition":0,"offset":5,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
''';
```

接着流式写入：

```sql
-- the stream name, should be uniq.
set streamName="streamExample";

-- load data as table
load jsonStr.`data` as datasource;

-- convert table as stream source
load mockStream.`datasource` options 
stepSizeRange="0-3"
as newkafkatable1;


select *  from newkafkatable1 
as table21;

-- output the the result to console.
save append table21  
as rate.`/tmp/delta/rate-1-table`
options mode="Append"
and duration="10"
and checkpointLocation="/tmp/rate-1" partitionBy key;
```

这里注意一下是流里面delta叫rate。

现在我们利用工具!delta查看已有的版本：

```sql
!delta history /tmp/delta/rate-2-table;
```

内容如下：

![](http://docs.mlsql.tech/upload_images/1063603-e43fba9ba7a22149.png)

现在我们可以对指定版本之前的数据做合并了：

```
!delta compact /tmp/delta/rate-2-table 8 1;
```

这条命令表示对第八个版本之前的所有数据都进行合并，每个目录（分区）都合并成一个文件。

我们看下合并前每个分区下面文件情况：

![](http://docs.mlsql.tech/upload_images/1063603-98a05bf000790a02.png)

合并后文件情况：
![](http://docs.mlsql.tech/upload_images/1063603-ba9292b2146633f1.png)

我们删除了16个文件，生成了两个新文件。另外在compaction的时候，并不影响读和写。所以是非常有用的。

# 同时加载多版本数据为一个表

Delta支持多版本，我们也可以一次性加载一个范围内的版本，比如下面的例子，我们说，将[12-14) 的版本的数据按
一个表的方式加载。接着用户可以比如可以按id做group by，在一行得到多个版本的数据。 

```sql
set a="b"; 

load delta.`/tmp/delta/rate-3-table` where 
startingVersion="12"
and endingVersion="14"
as table1;

select __delta_version__, collect_list(key), from table1 group by __delta_version__,key 
as table2;
```

# 查看表的状态

```sql
!delta info scheduler.time_jobs;
```

