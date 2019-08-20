# 如何设置流式计算回调

用户可以通过特定的命令查看一个流式程序的进度：

```sql
!show progress/streamExample;
```

如果你忘记了自己流程序的名字，那么可以使用

```sql
!show jobs;
```

获得列表。如果我想收集一个流程序什么时候开始，运行的状态，以及如果异常或者被正常杀死的事件，用户可以使用回调，具体使用方式如下：


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
核心关键点是：

```sql
!callback post "http://127.0.0.1:9002/api_v1/test" when "started,progress,terminated";
```

这个表示如果发生started,progress,terminated三个事件中的任何一个，都以HTTP POST协议上报给http://127.0.0.1:9002/api_v1/test接口。