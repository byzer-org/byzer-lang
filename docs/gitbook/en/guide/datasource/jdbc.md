# JDBC

## 加载数据

There are many datasource which can be connected by JDBC ，e.g. MySQL, Oracle,Hive thrift server.
In this chapter, we will show you how to load data from MySQL.

In order to create a connection ,you can use connect statement: 

```sql
 set user="root";
 
 connect jdbc where
 url="jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
 and driver="com.mysql.jdbc.Driver"
 and user="${user}"
 and password="${password}"
 as db_1;
 
```
Now, we get a connection called db_1, and the db_1 is also a alias name of wow(this name is the real db name in MySQL).
Once you have build connection, you can load the tables in db like following: 

```
load jdbc.`db_1.table1` as table1;
load jdbc.`db_1.table2` as table2;

select * from table1 as output;
```

Here are some parameters in connect statement:

| Property Name  |  Meaning |
|---|---|
|url|The JDBC URL to connect to. The source-specific connection properties may be specified in the URL. e.g., jdbc:postgresql://localhost/test?user=fred&password=secret|
|driver |The class name of the JDBC driver to use to connect to this URL.|
|partitionColumn, lowerBound, upperBound|	These options must all be specified if any of them is specified. In addition, numPartitions must be specified. They describe how to partition the table when reading in parallel from multiple workers. partitionColumn must be a numeric column from the table in question. Notice that lowerBound and upperBound are just used to decide the partition stride, not for filtering the rows in table. So all rows in the table will be partitioned and returned. This option applies only to reading.|
|numPartitions|	The maximum number of partitions that can be used for parallelism in table reading and writing. This also determines the maximum number of concurrent JDBC connections. If the number of partitions to write exceeds this limit, we decrease it to this limit by calling coalesce(numPartitions) before writing.|
|fetchsize|	The JDBC fetch size, which determines how many rows to fetch per round trip. This can help performance on JDBC drivers which default to low fetch size (eg. Oracle with 10 rows). This option applies only to reading.|
|batchsize|	The JDBC batch size, which determines how many rows to insert per round trip. This can help performance on JDBC drivers. This option applies only to writing. It defaults to 1000.|
|isolationLevel|	The transaction isolation level, which applies to current connection. It can be one of NONE, READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, or SERIALIZABLE, corresponding to standard transaction isolation levels defined by JDBC's Connection object, with default of READ_UNCOMMITTED. This option applies only to writing. Please refer the documentation in java.sql.Connection.|
|sessionInitStatement|	After each database session is opened to the remote DB and before starting to read data, this option executes a custom SQL statement (or a PL/SQL block). Use this to implement session initialization code. Example: option("sessionInitStatement", """BEGIN execute immediate 'alter session set "_serial_direct_read"=true'; END;""")|
|truncate|	This is a JDBC writer related option. When SaveMode.Overwrite is enabled, this option causes Spark to truncate an existing table instead of dropping and recreating it. This can be more efficient, and prevents the table metadata (e.g., indices) from being removed. However, it will not work in some cases, such as when the new data has a different schema. It defaults to false. This option applies only to writing.|
|createTableOptions|	This is a JDBC writer related option. If specified, this option allows setting of database-specific table and partition options when creating a table (e.g., CREATE TABLE t (name string) ENGINE=InnoDB.). This option applies only to writing.|
|createTableColumnTypes|	The database column data types to use instead of the defaults, when creating the table. Data type information should be specified in the same format as CREATE TABLE columns syntax (e.g: "name CHAR(64), comments VARCHAR(1024)"). The specified types should be valid spark sql data types. This option applies only to writing.|
|customSchema|	The custom schema to use for reading data from JDBC connectors. For example, "id DECIMAL(38, 0), name STRING". You can also specify partial fields, and the others use the default type mapping. For example, "id DECIMAL(38, 0)". The column names should be identical to the corresponding column names of JDBC table. Users can specify the corresponding data types of Spark SQL instead of using the defaults. This option applies only to reading.|

partitionColumn, lowerBound, upperBound,numPartitions are used to speed up the load performance.

You can use load statement without connect:

```sql
load jdbc.`db.table` options
and driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/...."
and user="..."
and password="...."
as table1;
```

You can use the raw SQL to fetch the data from MySQL and map the data to a new table:

```sql
load jdbc.`db_1.test1` where directQuery='''
select * from test1 where a = "b"
''' as newtable;

select * from newtable;
```

Please make sure the result should not too big.

## Save/Update data

Using connect firstly:

```sql
 set user="root";
 
 connect jdbc where
 url="jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
 and driver="com.mysql.jdbc.Driver"
 and user="${user}"
 and password="${password}"
 as db_1;
 
```

Save the data like this:

```
save append tmp_article_table as jdbc.`db_1.crawler_table`;
```

If you want to overwrite, the code may like this:

```
save overwrite ....
```


If you hope create a table first, try to use as follow:

```

run command as JDBC.`db_1` where 
driver-statement-0="drop table test1"
and driver-statement-1="create table test1.....";

save append tmp_article_table as jdbc.`db_1.test1`;
```

With this code, we drop the test1 ,then create it, and finally save data to it. 






