#How to load and save delta
#How to use delta in streaming applications

The essence of Delta is a directory in HDFS.

## Fundamental operations

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

You can write data into Delta lakes after running upper commands.

To load Delta：

```sql
load delta.`/tmp/delta/table10` as output;
```

## Support for Upsert semantic

Delta supports Upsert operations for data, semantic: update if exists, insert if not exists.

We've saved several data before, now run these commands:

```sql
set rawText='''
{"id":1,"content":"我更新了这条数据","label":0.0}
''';

load jsonStr.`rawText` as orginal_text_corpus;

save append orginal_text_corpus  
as delta.`/tmp/delta/table10` 
and idCols="id";
```

Data is been updated successfully.

![](http://docs.mlsql.tech/upload_images/WX20190819-192447.png)


## Streaming update support

We will show an example of streaming codes here, and don't focus too much on details, we will elaborate streaming coding in following chapters.

To finish this example, you need to start Kafka which's version must be higher than 0.10.0.

Firstly, write some data into Kafka through MLSQL:

```sql
set abc='''
{ "x": 100, "y": 201, "z": 204 ,"dataType":"A group"}
''';
load jsonStr.`abc` as table1;

select to_json(struct(*)) as value from table1 as table2;
save append table2 as kafka.`wow` where 
kafka.bootstrap.servers="127.0.0.1:9092";
```

Then start a streaming program to consume it:

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

set x and y as primary keys, then check the data：

```sql
load delta.`/tmp/delta/wow-0` as show_table1;
select * from show_table1 where x=100 and z=204 as output;
```

## compaction for small files

MLSQL is strict about small files in Delta, must be with append mode. MLSQL only compact small files that has not been updated.

Let's mock some data on Kafka:

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

Then load with streaming:

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

-- output the result to console.
save append table21  
as rate.`/tmp/delta/rate-1-table`
options mode="Append"
and duration="10"
and checkpointLocation="/tmp/rate-1" partitionBy key;
```
Attention, the rate here is in the streaming of Delta.

Now use !delta to check the version:

```sql
!delta history /tmp/delta/rate-2-table;
```

Output：

![](http://docs.mlsql.tech/upload_images/1063603-e43fba9ba7a22149.png)

Then compact data which's version is less than we get before:

```
!delta compact /tmp/delta/rate-2-table 8 1;
```
It will compact all the data that is less than version 8 and generate a new file.

Before compacting：

![](http://docs.mlsql.tech/upload_images/1063603-98a05bf000790a02.png)

After compacting：
![](http://docs.mlsql.tech/upload_images/1063603-ba9292b2146633f1.png)

It deletes 16 files and generate 2 new files. it does not affect write and read function when compact, so this feature is really useful.