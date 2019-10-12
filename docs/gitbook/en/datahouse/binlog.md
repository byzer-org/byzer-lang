# MySQL Binlog synchronization

Data warehouse needs to integrate offline or real-time business databases.Off-line mode is relatively simple, direct full synchronization and coverage.
The real-time mode is slightly more complex. Usually the following processes are used:

```
MySQL -> Cannel(Or some other tools) -> Kafka -> 

Streaming engine -> Hudi(HBase) -> 

Synchronize or dump > provide external data services.
```

The more tedious the technology, the more probable the problem will be and the more difficult the debugging will be. MLSQL provides a very simple solution:

```sql
MySQL -> MLSQL Engine -> Delta(HDFS)
```

The workload is mostly MLSQL scripts with two pieces of code, Load and Save.
The following is the Load statement:

```sql
set streamName="binlog";

load binlog.`` where 
host="127.0.0.1"
and port="3306"
and userName="xxx"
and password="xxxx"
and bingLogNamePrefix="mysql-bin"
and binlogIndex="4"
and binlogFileOffset="4"
and databaseNamePattern="mlsql_console"
and tableNamePattern="script_file"
as table1;
```

set streamName Represents that the script is streamed And the name of the process is binglog.
The load statement loads the binglog log into a table.

Attention should be paid to several parameters related to binglog:

1. bingLogNamePrefix    The prefix of MySQL binglog configuration. Business DBA can tell you.
2. binlogIndex    Consumption from the number of Binglogs.
3. binlogFileOffset Start consuming from where the binlog file is located.                   



Binlogfileoffset cannot specify any location. Because the location of binary files is difficult to locate with the naked eye.
For example, 4 indicates the starting position.
You can consult the DBA or view a reasonable location by following commands:

```sql
mysqlbinlog \ 
--start-datetime="2019-06-19 01:00:00" \ 
--stop-datetime="2019-06-20 23:00:00" \ 
--base64-output=decode-rows \
-vv  master-bin.000004
```
If an inappropriate location is randomly assigned, the result is that data cannot be consumed and incremental synchronization cannot be achieved.
The second set of parameters specifies the library name and table name:

1. databaseNamePattern  support Regular Expressions
2. tableNamePattern     support Regular Expressions


After getting table1, we need to synchronize to the Delta table.Note that we finally synchronize data instead of binlog logs. The code is as follows:

```sql
save append table1  
as binlogRate.`/tmp/binlog1/{db}/{table}` 
options mode="Append"
and idCols="id"
and duration="5"
and checkpointLocation="/tmp/cpl-binlog";
```

DB and table in `/tmp/binlog1/{db}/{table}` are placeholders.
Because it is possible to synchronize multiple tables in many databases at one time, the data source in the parameter binlogRate allows us to replace them by placeholders.

IdCols specifies a set of primary keys to enable MLSQL to complete the Upsert operation.
The last two parameters, duration and checkpoint location, are unique to streaming computing, which respectively indicate the running cycle and where the running logs are stored.
Now we have finished synchronizing any tables into Delta Data Lake!

At present, binlog synchronization has some limitations:

1. MySQL needs to configure binlog_format = Row. Of course, this is the default setting.
2. If the table structure is modified, synchronization will fail by default, and incremental synchronization will be needed after full synchronization.
   If you want to continue running, add mergeSchema= "true" setting to the Save statement.
3. If different tables have different primary key columns, multiple streaming synchronization scripts are required.

## Frequent Error

```
Trying to restore lost connectioin to .....
Connected to ....
```

You need to examine the server_id parameter configuration of the MySQL service my.cnf file.