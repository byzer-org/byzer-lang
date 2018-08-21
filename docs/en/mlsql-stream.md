## MLSQL Stream

MLSQL is a dsl covering Batch/Stream/Service API/Machine Learning. 
This document shows how to use MLSQL to write  stream application.

You can start a stream application just like running a batch script. 
All you need to do is paste the following script to page of "http://127.0.0.1:9003".

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
as jdbc.`mysql1.test1` 
options mode="Complete"
and `driver-statement-0`="create table  if not exists test1(k TEXT,c BIGINT)"
and `statement-0`="insert into wow.test1(k,c) values(?,?)"
and duration="3"
and checkpointLocation="/tmp/cpl3";

```


In real world uou can load kafka source like this:

```sql

-- if you are using kafka 1.0
load kafka.`pi-content-realtime-db` options 
`kafka.bootstrap.servers`="---"
as kafka_post_parquet;

-- if you are using kafka 0.8.0/0.9.0
load kafka9.`pi-content-realtime-db` options 
`kafka.bootstrap.servers`="---"
as kafka_post_parquet;

```

If you want to save data with static partition:

```sql

save append post_parquet  
as newParquet.`/table1/hp_stat_date=${date.toString("yyyy-MM-dd")}` 
options mode="Append" 
and duration="30" 
and checkpointLocation="/tmp/ckl1";
```

If you want to add watermark for a table:

```sql

select ..... as table1;

-- register watermark for table1
register WaterMarkInPlace.`table1` as tmp1
options eventTimeCol="ts"
and delayThreshold="1 seconds";

-- process table1
select count(*) as num from table1
group by window(ts,"30 minutes","10 seconds")
as table2;

save append ......
```


## How to manager your stream jobs?

1. Check all stream jobs in http://ip:port/stream/jobs/running
2. Kill any stream job in http://ip:port/stream/jobs/kill?groupId=....
