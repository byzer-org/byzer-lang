For now, MLSQL supports that using streamParquet,streamJDBC as the sink of stream. But there are a bunch of batch sink, we hope we can use them without more effects.

Again, we also hope we can operate the final output, it is very useful when we want to control the new data in every batch, for example, we count the data in every batch, and save them into parquet.

MLSQL provdes `a stream sub batch query` mode, means you can write a nested  MLSQL script in a stream query.

Here is the example code:

```sql
-- the stream name, should be uniq.
set streamName="streamExample";


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
select cast(value as string) as k  from newkafkatable1
as table21;

-- run command as  MLSQLEventCommand.`` where
--       eventName="started,progress,terminated"
--       and handleHttpUrl="http://127.0.0.1:9002/jack"
--       and method="POST"
--       and params.a=""
--       and params.b="";
!callback post "http://127.0.0.1:9002/api_v1/test" when "started,progress,terminated";
-- output the the result to console.


save append table21  
as custom.`` 
options mode="append"
and duration="15"
and sourceTable="jack"
and code='''
select count(*) as c from jack as newjack;
save append newjack as parquet.`/tmp/jack`; 
'''
and checkpointLocation="/tmp/cpl15";

```

The only difference is in the last save statement. the new options in where are:

```
sourceTable, this means we will register the final output of the stream as a table.
code, how to operate the sourceTable
```
Also notice that the format is not streamJDBC or streamParquet, it's `custom`. 

The MLSQL script in the code block is for batch mode, and for now, you should inherit anything from the stream context except the sourceTable.


