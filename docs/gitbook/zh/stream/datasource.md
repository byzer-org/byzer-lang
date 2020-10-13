# Kafka和MockStream

MLSQL 目前显式支持Kafka 以及MockStream. MockStream主要用于模拟数据源，测试场景中应用的比较多。


> 值得注意的是，MLSQL支持 Kafka 0.8,0.9以及1.x版本，而原生的struectured streaming只支持0.10版本的Kafka.

如果你使用的 kafka > 0.10：

```sql
load kafka.`topic_name` options
`kafka.bootstrap.servers`="---"
as kafka_post_parquet;
```

如果你使用的kafka 为 0.8/0.9 请使用：


```sql
load kafka8.`topic_name` options
`kafka.bootstrap.servers`="---"
as kafka_post_parquet;
```

获取了数据之后，MLSQL Stream支持的输出数据源有：
 
1. 文件写入(比如 parquet,orc,json,csv等等)
2. Kafka写入
3. 以及MySQL写入。

具体使用如下：

```
connect jdbc where  
driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/wow"
and user="---"
and password="----"
as mysql1;

save append table21  
as streamJDBC.`mysql1.test1` 
options mode="Complete"
and `driver-statement-0`="create table  if not exists test1(k TEXT,c BIGINT)"
and `statement-0`="insert into wow.test1(k,c) values(?,?)"
and duration="3"
and checkpointLocation="/tmp/cpl3";
```

只有save语法会触发整个流的提交和执行。这里面有几个核心的参数：

1. duration，执行周期，单位为秒,如果是0,则执行完立马执行下一个周期。
2. checkpointLocation 流重启后恢复用
3. mode， 三种模式，Update,Append,Complete,请参考structured streaming里这三种模式的区别。

## 模拟输入数据源

为了方便测试，我们提供了一个Mock输入，来模拟Kafka输入。

```
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
```

通过set 以及load语法我们制造了一批数据，然后呢，我们使用mockStream来加载这些数据，mockStream
会每个周期发送0到3条数据出来，这个通过stepSizeRange进行控制。

这样我们就可以脱离Kafka从而实现方便的代码测试。当然，你肯定很像知道newkafkatable1也就是我们加载完kafka后的数据该怎么处理，
和普通的批处理是类似的：

```sql
select cast(key as string) as k,count(*) as c  from newkafkatable1 group by key
as table21;
```

下面是一个典型的流式程序：


```sql
-- the stream name, should be uniq.
set streamName="streamExample";

-- connect mysql as the data sink.
connect jdbc where  
driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/wow"
and driver="com.mysql.jdbc.Driver"
and user="---"
and password="----"
as mysql1;


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
-- save append table21  
-- as console.`` 
-- options mode="Complete"
-- and duration="10"
-- and checkpointLocation="/tmp/cpl3";

-- save the data to mysql.
save append table21  
as streamJDBC.`mysql1.test1` 
options mode="Complete"
and `driver-statement-0`="create table  if not exists test1(k TEXT,c BIGINT)"
and `statement-0`="insert into wow.test1(k,c) values(?,?)"
and duration="3"
and checkpointLocation="/tmp/cpl3";
```

注意：

> 任何一个流程序都需要一个唯一的标识符，通过  set streamName="streamExample"; 来设置。


如果你不想有任何依赖，就是想跑一个例子看看，可以使用如下的语句：

```sql
-- the stream name, should be uniq.
set streamName="streamExample";

-- mock some data.
set data='''
{"key":"yes","value":"a,b,c","topic":"test","partition":0,"offset":0,"timestamp":"2008-01-24 18:01:01.001","timestampType":0}
{"key":"yes","value":"d,f,e","topic":"test","partition":0,"offset":1,"timestamp":"2008-01-24 18:01:01.002","timestampType":0}
{"key":"yes","value":"k,d,j","topic":"test","partition":0,"offset":2,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"m,d,z","topic":"test","partition":0,"offset":3,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"o,d,d","topic":"test","partition":0,"offset":4,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"m,m,m","topic":"test","partition":0,"offset":5,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
''';

-- load data as table
load jsonStr.`data` as datasource;

-- convert table as stream source
load mockStream.`datasource` options 
stepSizeRange="0-3"
and valueFormat="csv"
and valueSchema="st(field(column1,string),field(column2,string),field(column3,string))"
as newkafkatable1;

-- aggregation 
select column1,column2,column3,kafkaValue from newkafkatable1 
as table21;

-- output the the result to console.
save append table21  
as console.`` 
options mode="Append"
and duration="10"
and checkpointLocation="/tmp/cpl3";
```

输出结果如下：


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


注意：

> 使用console，每次重启你需要删除checkpointLocation
  







