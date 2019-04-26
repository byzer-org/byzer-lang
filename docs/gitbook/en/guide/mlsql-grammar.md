## MLSQL Grammar

MLSQL supports the following statements:

1. connect  
2. set    
3. select 
4. train/predict/run
5. register 
6. save    
7. include
8. !      --(macros)

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

![](https://github.com/allwefantasy/streamingpro/raw/master/images/WX20181113-144350.png)

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
8.  kafka/kafka8/kafka9(stream program)
9.  crawlersql
10. image
11. jsonStr
12. mockStream (stream program)
13. jdbc (stream program)
14. newParquet(stream program)

We will show you how to use jsonStr:

```sql
set data='''
{"key":"yes","value":"no","topic":"test","partition":0,"offset":0,"timestamp":"2008-01-24 18:01:01.001","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":1,"timestamp":"2008-01-24 18:01:01.002","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":2,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":3,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":4,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":5,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
''';

-- load json string in set statement as table.
load jsonStr.`data` as datasource;

select * from datasource as output;
```

mockStream format is applied in stream program, it makes stream application is easy to test:

```sql

-- stream name
set streamName="streamExample";

set data='''
{"key":"yes","value":"no","topic":"test","partition":0,"offset":0,"timestamp":"2008-01-24 18:01:01.001","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":1,"timestamp":"2008-01-24 18:01:01.002","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":2,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":3,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":4,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"no","topic":"test","partition":0,"offset":5,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
''';

-- load json string in set statement as table.
load jsonStr.`data` as datasource;

-- convert the table as stream source
load mockStream.`datasource` options 
-- mockStream will fetch data with the number 0-2 from datasource，according to the offset.
stepSizeRange="0-3"
as newkafkatable1;

```

Load statement  also supports helping user find the doc about Estimator/Transformer in MLSQL:

```
-- check available params of module
load modelParams.`RandomForest`
as output;

-- check the doc and example of module
load modelExample.`RandomForest`
as output;

-- check the params of model
load modelExplain.`/tmp/model` where
alg="RandomForest" as outout;


```


Here are some load file examples:

```sql
-- raw text without schema
load text.`/Users/allwefantasy/CSDNWorkSpace/tmtagsHistory`
as table1;

load hive.`db.table` as table1;

load parquet.`path` as  table1;

load csv.`path` where
header=“true”
and interSchema=“true”
as table1;

load json.`path` as table1;

```

Crawler example:

```
load crawlersql.`https://www.csdn.net/nav/ai`
options matchXPath="//ul[@id='feedlist_id']//div[@class='title']//a/@href"
and fetchType="list"
and `page.type`="scroll"
and `page.num`="10"
and `page.flag`="feedlist_id"
as aritle_url_table_source;
```

ES Example:

```
connect es where `es.nodes`=“esnode1"
and `es.net.http.auth.user`=""
and `es.net.http.auth.pass`=""
as es1;

load es.`es1.pi_top/dxy` as es_pi_top;

```

HBase Example:

```
load hbase.`wq_tmp_test1`
options rowkey="id"
and zk="---"
and inputTableName="wq_tmp_test1"
and field.type.filed1="FloatType"
and field.type.filed2="TimestampType"
and field.type.filed3="BinaryType"
as testhbase
;
```

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

### train/predict/run

![](https://github.com/allwefantasy/streamingpro/raw/master/images/WX20181111-201226@2x.png)

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

We have so many modules can be used in train statement is wait to you to dig. [Using Build-in Algorithms](https://github.com/allwefantasy/streamingpro/blob/master/docs/en/mlsql-build-in-algorithms.md)

`predict` and `run` statement have the same syntax with train. run is used when you just want to transform data. predict is used to transform data with model trained before.

```sql
predict orginal_text_corpus as Word2VecInPlace.`/tmp/word2vecinplace` as output;
```


### register

![](https://github.com/allwefantasy/streamingpro/raw/master/images/WX20181113-144806.png)
![](https://github.com/allwefantasy/streamingpro/raw/master/images/WX20181113-144817.png)

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

Register statement is also can be used to register UDF.

Here is one python example:

```sql
register ScriptUDF.`` as echoFun options
and lang="python"
and dataType="map(string,string)"
and code='''

def apply(self,m):
    return m

'''
;

select echoFun(map(“a”,”b”) ) as res as output;
```

More detail please refer [here](https://github.com/allwefantasy/streamingpro/blob/master/docs/en/mlsql-script-support.md)

Register can convert things like follow to UDF：

1. Scala snippet
2. Python snippet
3. The model generated by train statement


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

-- set variable with multi lines
set xx = '''
sentence1
sentence2
sentence3
''';

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

Here are some examples may help you to understand how to use set statement:

```sql
-- sql type also support variable evaluation.
set end_ts = "2018-08-22 16:12:00";

set end_ut = `SELECT unix_timestamp('${end_ts}', 'yyyy-MM-dd HH:mm:ss') * 1000` 
options type="sql";
set start_ut = `SELECT (unix_timestamp('${end_ts}', 'yyyy-MM-dd HH:mm:ss') - 30 * 60) * 1000` 
options type="sql";

select "${start_ut}" as a, "${end_ut}" as tt as output;

-- multi line set statment also supports
-- date is a build-in variable so you can easyly get a formatted date.
set jack='''
 hello today is:${date.toString("yyyyMMdd")}
''';

-- conf
set hive.exec.dynamic.partition.mode=nonstrict  options type = "conf" ;

--  shell

set t = `date`  options type = "shell";

```

### include

Suppose you have file `/tmp/a.mlsql` contains:

```sql
--加载脚本
load script.`plusFun` as scriptTable;
--注册为UDF函数 名称为plusFun
register ScalaScriptUDF.`scriptTable` as plusFun options
className="PlusFun"
and methodName="plusFun"
;

-- 使用plusFun
select plusFun(1,1) as res as output;
```


Then you can include this file in your MLSQL script.

```sql
-- 填写script脚本
set plusFun='''
class PlusFun{
  def plusFun(a:Double,b:Double)={
   a + b
  }
}
include hdfs.`/tmp/a.sql`;
''';
```

Include also supports http protocol.  Do like this:

```sql
include http.`http://abc.com` options 
`param.a`="coo" 
and `param.b`="wow";
and method="GET"
```

when option starts with "param." ,then this parameter will be send to the request address. Here is the real request:

```
curl -XGET 'http://abc.com?a=coo&b=wow'
```

If you send request to StreamingPro server with parameters like this:

```
context.__default__include_fetch_url__=http://abc.com
context.__default__include_project_name__=analyser....
```

Include http source will use  `context.__default__include_fetch_url__` as the target url, so you can use include like follow:
 
```
include http.`function.dir1.scriptfile1` ;
```
the request behind will like this：

```
curl -XGET 'http://abc.com?a=coo&b=wow&path=function.dir1.scriptfile1&projectName=analyser'
```

### !

We can convert a bunch of MLSQL script code to a special command, and you can use ! to execute the command. Here is the example:

```
set findEmailByName = '''
-- notice that there are no semicolon in the end of this line. 
select email from table1 where name="{}" 
''';

-- Execute it like a command.
!findEmailByName "jack";
```





