There are two ways to get information about the stream job in MLSQL engine.
The first way is using `!show` command:

```sql
!show progress/streamExample;
```

Also you can use `!show  jobs;` to get job list and `!kill jobName` to terminate your job. But sometimes you hope you can set some callback URL, once the stream job started, progressed or terminated, you can get notified. This MPIP provides the callback function in stream job.

Example code:

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
select cast(key as string) as k,count(*) as c  from newkafkatable1 group by key
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
as console.`` 
options mode="Complete"
and duration="15"
and checkpointLocation="/tmp/cpl14";

```

the key is this line:

```
!callback post "http://127.0.0.1:9002/api_v1/test" when "started,progress,terminated";
```

this means we set a callback when there are some events like started,progress,terminated happen, then post a message to URL `http://127.0.0.1:9002/api_v1/test`.


