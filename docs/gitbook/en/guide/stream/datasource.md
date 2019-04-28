# Datasource

MLSQL for now only support kafka as stream source.


If the  kafka version > 0.10：

```sql
load kafka.`topic_name` options
`kafka.bootstrap.servers`="---"
as kafka_post_parquet;
```

otherwise:


```sql
load kafka8.`topic_name` options
`kafka.bootstrap.servers`="---"
as kafka_post_parquet;
```

The sink for now supports:

1. streamParquet
2. StreamJDBC
3. Kafka


Here is the example:

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

Only the save will trigger the whole script is submitted. You should make sure
the following parameters available:

1. duration， if set 0, no wait. 
2. checkpointLocation 
3. mode， thee modes:Update,Append,Complete.

Please refer structured streaming document for more detail.

## 模拟输入数据源

You can use mockStream to mock kafka which make you test more easy.


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

We mock some data with set/load statement,and use mockStream to simulate the kafka
to emit data. Every scheduler round, mockStream will send 0 to 3 messages(which is controlled 
by stepSizeRange).  

then we cast key,value from byte array to string, just like we process data from kafka:

```sql
select cast(key as string) as k,count(*) as c  from newkafkatable1 group by key
as table21;
```

Here is a example where you write you stream data to MySQL:


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

Notice：

> MLSQL stream needs a uniq token, try use like this `set streamName="streamExample";` to set it properly.


The following is a completely, runnable code example:

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

The output like this:


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

> If the sink is console, you may need to delete checkpointLocation after  any restart.
  







