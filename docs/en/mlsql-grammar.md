## MLSQL Grammar

MLSQL supports following statements:

1. connect  
2. set    
3. select 
4. train  
5. register 
6. save     

### connect

The connect statement is used to connect external storage engine eg. MySQL, ElasticSearch with some options.

Connect Syntax:

```
('connect'|'CONNECT') format 
'where'? expression? booleanExpression* ('as' db)?
```

Here, `format` is the name of the target storage engine you want to connect ,and  
`expression` and  `booleanExpression` is used to provide extra information like username, passowrd so MLSQL can establish the 
connection to the target.

Finally , the `db`  gives the target a name which can be seen as alias to this connection.
  
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

-- of course ,you can operate mysql wihout connect statment. 
load jdbc.`mysql-1.table1` options
and driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/...."
and driver="com.mysql.jdbc.Driver"
and user="..."
and password="...."
as table1;

```

This example shows how it brings the convenient to operate the third-party storage engine.


### load 

In MLSQL, you can load almost anything as a table. The load statement is used to achieve this target. 

Load syntax:

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
5.  es    (requires elasticsearch-hadoop jar) 
6.  hbase (requires streamingpro-hbase jar)
7.  redis (requires streamingpro-redis jar)
8.  kafka/kafka8/kafka9
9.  crawlersql
10. image

### select 
 
The select statement is used to select data from table which is loaded by load statement. 
Hive table is no need to load, you can use select to query directly.

Select syntax:

```
('select'|'SELECT') ~(';')* 'as' tableName
```

Select statement is a standard spark SQL except ends with `as tableName` suffix, this means any select statement return as 
table.
   
   
Example:

```sql
select a.column1 from 
(select column1 from table1) a left join b on a.column1=b.column2 
as newtable1;
```

This statement is kind of complex, but if you remove the "as newtable1", you will find that it's a standard SQL.
In MSLQL, select is good at processing data. There is also another statement is more powerful in processing data then select,
we will introduce it later. 

### train 

Train statement not only  provide you  the ability to process data, but also train any machine learning algorithm. MLSQL 
have many modules implemented which can be used in train statement.  
 
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

MLSQL loads libsvm data from HDFS/Local File System, and then train it with  RandomForest implemented in Spark MLLib.
Finally the model would be saved in directory `/tmp/model`.

Here is another example to show how to processing data:


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
and resultFeature="flat"
;
```

You can get converting result from `/tmp/word2vecinplace/data`. Let me show you how to do this:
 
```sql
load parquet.`/tmp/word2vecinplace/data` as converting_result;
select * from converting_result as output;
```

You can also get predict function from `/tmp/word2vecinplace`(The register statement we will learn later):

```sql
register Word2VecInPlace.`/tmp/word2vecinplace` as word2vec;
select word2vec(content) from sometable as output;
```

### register

Register statement is used together with train statement. Train statement provides model, and Register statement create function
from the model.

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
('save'|'SAVE') (overwrite | append | errorIfExists |ignore)* tableName 'as' format '.' path 'options'? expression? booleanExpression* ('partitionBy' col)? 
```

Example:

```sql
save overwrite  table1 as csv.`/tmp/data` options
header="true";
```

MLSQL will save table1 to `/tmp/data` with csv format. It also keep the header in csv using `header="true"` declared 
in options.
  
### set 
  
Set statement is used as variable declared.
  
Set syntax:
  
```
('set'|'SET') setKey '=' setValue 'options'? expression? booleanExpression*
```

Example:

```sql
-- here we set a xx variable.
set  xx = `select unix_timestamp()` options type = "sql" ;

-- you can use it in select statment with `${}` format.
select '${xx}'  as  t as tm;

-- you can also use it in path in save statement
save overwrite tm as parquet.`hp_stae=${xx}`;
```

set support three types:

1. sql  
2. shell 
3. conf

SQL,Shell means MLSQL will translate the statement with sql/shell interpreter.
When you use conf type, MLSQL will just treat is as string.







 
 