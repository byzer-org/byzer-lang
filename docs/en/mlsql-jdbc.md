## MLSQL JDBC Support

Here is the script example show you how to deal with MySQL(we only have tested with MySQL).


```sql
-- prepare the connection
connect jdbc where driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/wow"
and driver="com.mysql.jdbc.Driver"
and user="---"
and password="---"
as db1;

-- prepare test data
set testdata='''
{"a":"a","b":"b"}
{"a":"a","b":"2"}
{"a":"m","b":"2"}
''';

-- load test data as table
load jsonStr.`testdata` as testdata;

-- create table if not exists.
-- Actully you can do any DLL with JDBC module.

train testdata as JDBC.`db1` where
`driver-statement-0`="CREATE TABLE if NOT exists abc(a varchar(255),b text,CONSTRAINT a_index UNIQUE (a)) ENGINE = InnoDB";

-- save data to MySQL 
-- Notice that when idCol is provied then we will 
-- use upsert mode.
-- Upsert mode depends the `onducplicate key update` grammar of MySQL. 
-- So please make sure you idCol is created with UNIQUE CONSTRAINT.
-- Also, idCol support multi-column combination.
save append testdata
as jdbc.`db1.abc`
options idCol="a";

-- check the result
load jdbc.`db1.abc` as tbs;
```

the result should be like this:

|a|b|
|---|---|
|m |2|
|a |b|


Except that already mentioned, JDBC sink applied in structured stream is also supported. Here we will show you how to do this.

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
-- the key starts with `driver` means that the statement will be executed in driver.
and `driver-statement-0`="create table  if not exists test1(k TEXT,c BIGINT)"
-- statment-0 will be executed in every executor.
-- the number behind the statment means you can configure several statments 
-- then the system will execute according the number with ascendant order.
and `statement-0`="insert into wow.test1(k,c) values(?,?)"
and duration="3"
and checkpointLocation="/tmp/cpl3";
```



