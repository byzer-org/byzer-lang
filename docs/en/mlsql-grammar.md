## MLSQL Grammar

MLSQL supports the following statements:

1. connect  
2. set    
3. select 
4. train  
5. register 
6. save     

### connect

The connect statement is used to connect the external storage engines ie. MySQL, ElasticSearch etc. with options.

Connect Syntax:

```
('connect'|'CONNECT') format 
'where'? expression? booleanExpression* ('as' db)?
```

Here, `format` is the name of the target storage engine you want to connect.   
`expression` and  `booleanExpression` are used to provide extra information such as username and password such that MLSQL can connect to the target. Finally, the `db` is an alias to this connection.
  
Demo:
  
```sql
connect jdbc where  
truncate="true"
and driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/...."
and driver="com.mysql.jdbc.Driver"
and user="..."
and password="...."
as mysql-1;

-- then you can operate mysql-1 without any extra information
load jdbc.`mysql-1.table1` as table1;
select * from table1 as output;

-- of course, you can operate mysql without connect statement. 
load jdbc.`mysql-1.table1` options
and driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/...."
and driver="com.mysql.jdbc.Driver"
and user="..."
and password="...."
as table1;
```

This example demonstrates how convenient it is to integrate with the third-party storage engine.


### load 

In MLSQL, you can load almost anything as a table. The load syntax is as follows:
```
('load'|'LOAD') format '.' path 'options'? expression? booleanExpression*  'as' tableName
```

Example:

```sql
load csv.`/tmp/abc.csv` as table1;
```

Here are the format we build-in support:

1.  csv
2.  parquet
3.  orc
4.  jdbc  
5.  es (requires elasticsearch-hadoop jar) 
6.  hbase (requires streamingpro-hbase jar)
7.  redis (requires streamingpro-redis jar)
8.  kafka/kafka8/kafka9
9.  crawlersql
10. image

### select 
 
The select statement is used to select data from table loaded by load statement. 
You do not need to load the Hive table, which you can use select to query directly.

Select syntax:

```
('select'|'SELECT') ~(';')* 'as' tableName
```

Select statement is a standard spark SQL statement except that it ends with `as tableName` suffix. This means any select statement will return a table as a result.
   
Example:

```sql
select a.column1 from 
(select column1 from table1) a left join b on a.column1=b.column2 
as newtable1;
```

This statement looks complex. However, if you remove the "as newtable1", you will find that it's a standard SQL.
In MSLQL, select is good at processing data. There is also another statement train that is more powerful than select in data processing data. We will introduce it in the next section. 

### train 

Train statement provides you not only the ability to process data but also train any machine learning algorithms. MLSQL has many modules implemented which can be used in the train statement.  
 
Train syntax: 

```
('train'|'TRAIN') tableName 'as' format '.' path 'where'? expression? booleanExpression* 
```

Example:

```sql
load libsvm.`/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;
train data as RandomForest.`/tmp/model` 
where maxDepth="3";
```

MLSQL loads libsvm data from the HDFS/Local File System, and then train it with RandomForest in Spark MLLib.
Finally the model would be saved in the directory `/tmp/model`.

Here is another example showing how to process data:

```sql
load parquet.`/tmp/data`
as orginal_text_corpus;

-- convert text to wordembbeding presentation
train orginal_text_corpus as Word2VecInPlace.`/tmp/word2vecinplace`
-- inputColumn
where inputCol="content"
-- stopwords
and stopWordPath="/tmp/tfidf/stopwords"
-- flatt hresult
and resultFeature="flat";
```

You can find the converting result in `/tmp/word2vecinplace/data`. Let me show you how to do this:
 
```sql
load parquet.`/tmp/word2vecinplace/data` as converting_result;
select * from converting_result as output;
```

You can also get predict function from `/tmp/word2vecinplace`(We will discuss the register statement later):

```sql
register Word2VecInPlace.`/tmp/word2vecinplace` as word2vec;
select word2vec(content) from sometable as output;
```

### register

Register statement is often used together with train statement. Train statement provides a model, and Register statement create a function from the model.

Register syntax: 

```
('register'|'REGISTER') format '.' path 'as' functionName 'options'? expression? booleanExpression* 
```

Example:

```sql
load libsvm.`/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;

train data as RandomForest.`/tmp/model` 
where maxDepth="3";

register RandomForest.`/tmp/model` as rf_predict;
select rf_predict(feature) from data as output;
```

### save
 
Save is used to write table to any storage. 

Save syntax: 

```
('save'|'SAVE') (overwrite | append | errorIfExists | ignore)* tableName 'as' format '.' path 'options'? expression? booleanExpression* ('partitionBy' col)? 
```

Example:

```sql
save overwrite table1 as csv.`/tmp/data` options
header="true";
```

MLSQL will save table1 to `/tmp/data` in csv format. It also keeps the header in csv using the option `header="true"`.
  
### set 
  
Set statement is used for variable declarations.
  
Set syntax:
  
```
('set'|'SET') setKey '=' setValue 'options'? expression? booleanExpression*
```

Example:

```sql
-- here we set a xx variable.
set  xx = `select unix_timestamp()` options type = "sql" ;

-- you can use it in select statement with `${}` format.
select '${xx}' as t as tm;

-- you can also use it in the save statement
save overwrite tm as parquet.`hp_stae=${xx}`;
```

set supports three types:

1. sql  
2. shell 
3. conf

SQL and Shell mean that MLSQL will translate the statement with sql/shell interpreter, while conf means MLSQL will just treat it as string.