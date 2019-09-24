# 流结果输出到WebConsole

流的一个很大缺点是输出不直观，不像交互式，跑完就能在Console看到结果. MLSQL提供了`webConsole`,方便大家
调试。

我们新建一个批脚本，方便往Kafka写数据：

```sql
set abc='''
{ "x": 100, "y": 200, "z": 200 ,"dataType":"A group"}
{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
{ "x": 100, "y": 100, "z": 260 ,"dataType":"B group"}
{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
{ "x": 100, "y": 100, "z": 260 ,"dataType":"B group"}
{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
{ "x": 100, "y": 100, "z": 260 ,"dataType":"B group"}
{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
''';
load jsonStr.`abc` as table1;

select to_json(struct(*)) as value from table1 as table2;
save append table2 as kafka.`wow` where 
kafka.bootstrap.servers="127.0.0.1:9092";

```

接着写一个流脚本：

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
   as webConsole.`` 
   options mode="Append"
   and idCols="x,y"
   and dropDuplicate="true"
   and duration="5"
   and checkpointLocation="/tmp/s-cpl7";

```

打开两个标签页，启动流，时不时点下批，在console下端就能看到如下结果：

```sql
19/09/20 11:07:05  INFO MLSQLConsoleWriter: [owner] [allwefantasy@gmail.com] [groupId] [41613ea5-8db9-4bf9-a91e-c739387af2d3]  kafkaValue | [, 1, 12, 2019-09... 
19/09/20 11:07:05  INFO MLSQLConsoleWriter: [owner] [allwefantasy@gmail.com] [groupId] [41613ea5-8db9-4bf9-a91e-c739387af2d3] -RECORD 6--------------------------
19/09/20 11:07:05  INFO MLSQLConsoleWriter: [owner] [allwefantasy@gmail.com] [groupId] [41613ea5-8db9-4bf9-a91e-c739387af2d3]  dataType   | B group              
19/09/20 11:07:05  INFO MLSQLConsoleWriter: [owner] [allwefantasy@gmail.com] [groupId] [41613ea5-8db9-4bf9-a91e-c739387af2d3]  x          | 100                  
19/09/20 11:07:05  INFO MLSQLConsoleWriter: [owner] [allwefantasy@gmail.com] [groupId] [41613ea5-8db9-4bf9-a91e-c739387af2d3]  y          | 100                  
19/09/20 11:07:05  INFO MLSQLConsoleWriter: [owner] [allwefantasy@gmail.com] [groupId] [41613ea5-8db9-4bf9-a91e-c739387af2d3]  z          | 260                  
19/09/20 11:07:05  INFO MLSQLConsoleWriter: [owner] [allwefantasy@gmail.com] [groupId] [41613ea5-8db9-4bf9-a91e-c739387af2d3]  kafkaValue | [, 1, 13, 2019-09... 
19/09/20 11:07:05  INFO MLSQLConsoleWriter: [owner] [allwefantasy@gmail.com] [groupId] [41613ea5-8db9-4bf9-a91e-c739387af2d3] -RECORD 7--------------------------
19/09/20 11:07:05  INFO MLSQLConsoleWriter: [owner] [allwefantasy@gmail.com] [groupId] [41613ea5-8db9-4bf9-a91e-c739387af2d3]  dataType   | B group              
19/09/20 11:07:05  INFO MLSQLConsoleWriter: [owner] [allwefantasy@gmail.com] [groupId] [41613ea5-8db9-4bf9-a91e-c739387af2d3]  x          | 120                  
19/09/20 11:07:05  INFO MLSQLConsoleWriter: [owner] [allwefantasy@gmail.com] [groupId] [41613ea5-8db9-4bf9-a91e-c739387af2d3]  y          | 100                  
19/09/20 11:07:05  INFO MLSQLConsoleWriter: [owner] [allwefantasy@gmail.com] [groupId] [41613ea5-8db9-4bf9-a91e-c739387af2d3]  z          | 260                  
19/09/20 11:07:05  INFO MLSQLConsoleWriter: [owner] [allwefantasy@gmail.com] [groupId] [41613ea5-8db9-4bf9-a91e-c739387af2d3]  kafkaValue | [, 1, 14, 2019-09... 
```