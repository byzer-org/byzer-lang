MLSQL Version 1.6.0 (2020-06-29)
==========================

这次版本憋了大半年了。核心code已经基本不变，并且稳定下来。更多的是新插件扩展MLSQL的生态。这次时间很长的最主要原因是希望能多积累点bug,然后fix掉，获取一个更稳定的版本。

## 语言层面

新增代码提示插件，配合新版本的console,大家就可以体验到MLSQL的代码提示了。我们发现他也可以单独部署，所以同时也独立出一个项目。

[sql-code-intelligence](https://github.com/allwefantasy/sql-code-intelligence)

## 接口层面

我们新增加了 [mlsql-jdbc](https://github.com/allwefantasy/mlsql-jdbc),这样除了http以外，MLSQL也能对接jdbc请求了。

另外对于Rest接口，我们新增了 `includeSchema`参数,会返回json的schema信息，而无需使用端通过json进行推测。


## 插件体系

1. 插件元信息存储支持delta和MySQL了。MySQL能够获得更好的启动性能。
2. 插件安装可以指定版本安装

## 启动保护

启动时，可能系统还没有完全初始化好，这个时候如果请求进来可能会导致部分初始化功能无法完成。1.6.0版本提供了保护，会hang住请求直到初始化完成。

## 适配了Spark 3.0.0版本

目前只是MLSQL Core进行了适配，目前MLSQL的很多插件比如spark-binlog/delta-plus/pyjava等都还没有完成适配。但是普通的流批是没有问题的。我们将在1.7.0版本完成插件的适配。

## 较为完善的Python支持

MLSQL Engine可以支持执行Python项目，以及支持Ray的数据互通。

## 以及很多Bug修复
不再列举

MLSQL Version 1.5.0 (2020-01-xx)
==========================





MLSQL Version 1.4.0 (2019-09-xx)
==========================

MLSQL Language
--------
- [MLSQL Analyzer](http://docs.mlsql.tech/zh/grammar/analyze.md) 

Bug Fix
---------

- When extracting HBase namespace, it returns ref instead of real namespace.  
- When using ET JDBC to update MySQL, Losing data will happens. 
- more......  
 


New Features
----------
- Python 
    - [Tensorflow Cluster](http://docs.mlsql.tech/zh/python/dtf.html)
    - [Python Interactive Mode](http://docs.mlsql.tech/zh/python/interactive.html)
    - [Python Table Mode](http://docs.mlsql.tech/zh/python/table.html)
- Batch
    - [AdHoc Kakfa](http://docs.mlsql.tech/zh/stream/query_kafka.html)
- Stream
    - [Kafka Schema Infer](http://docs.mlsql.tech/zh/stream/data_convert.html)
    - [Kafka tools](http://docs.mlsql.tech/zh/stream/kakfa_tool.html)    
                               
Break Features 
--------

- Remove streamingpro-opencv, streamingpro-dl4j, streamingpro-automl
- Merge streamingpro-crawler into stremingpro-mlsql
- `/stream/jobs/kill` and `/stream/jobs/running`,`/run/sql` are removed


Components
--------

- [PyJava](https://github.com/allwefantasy/pyjava) is an ongoing effort towards bringing the data exchanging ability between Java/Scala and Python.
-  MLSQL Console supports pyechars render.
- [MLSQL Console supports notebook mode](http://docs.mlsql.tech/zh/console/notebook.html)  

Plan of next release.
-------
- Python support should be more robust
 

Docs Link
--------

- [MLSQL-1.4.0](http://docs.mlsql.tech/v1.4.0/zh/) 

Download Link
---------

- [MLSQL-1.4.0](http://download.mlsql.tech/1.4.0/)



MLSQL Version 1.3.0 (2019-06-xx)
==========================

Break Features 
--------

- Remove Spark 2.2.x support

New Features
--------

- [Dynamically increase/decrease engine resource](http://docs.mlsql.tech/en/guide/et/resource.md)
- [Table cache](http://docs.mlsql.tech/en/guide/et/CacheExt.md) 
- sessionPerRequest  create new session for every request.
- directQuery Auth   support directQuery auth.
- set statement sql auth
- [Stream callback](http://docs.mlsql.tech/en/guide/stream/callback.html)
- [Stream batch mode(for sink)](http://docs.mlsql.tech/en/guide/stream/subquery.html)
- [MySQL Binlog datasource](http://docs.mlsql.tech/en/guide/stream/binlog.html)
- [Delta plus](http://docs.mlsql.tech/en/guide/datasource/delta_plus.md)
- [Stream schema infer](http://docs.mlsql.tech/en/guide/stream/infer_schema.md)
- [Kafka tool](http://docs.mlsql.tech/en/guide/stream/kafka_tool.md)
- Script progress track  you can use "load __mlsql__.`/jobs/get/[jobid]` as output" to get the script processing. 



MLSQL Version 1.2.0 (2019-04-14)
==========================

MLSQL Language
--------

- Adding new symbol `!` to execute command in MLSQL.
- Grammar validate before really execute MLSQL script. 

Bug Fix
---------

- [PR-1011 Jython udf do not support null parameter](https://github.com/allwefantasy/streamingpro/pull/1011)
- [PR-1010 Kill cannot kill job when sessionPerUser enabled](https://github.com/allwefantasy/streamingpro/pull/1010)
- [PR-1000 System show jobs without stream jobs.](https://github.com/allwefantasy/streamingpro/pull/1000)

New Features
----------

- [Compile time /Select Statement runtime auth support](https://github.com/allwefantasy/streamingpro/pull/990)
- [UDF written by Java support](https://github.com/allwefantasy/streamingpro/pull/911)

Break Features 
--------

- Use streamJDBC instead of JDBC in stream sink.
- Use streamParquet instead of parquet in stream sink.

Components
--------

- [MLSQL Console](https://github.com/allwefantasy/mlsql-api-console) can be used with MLSQL Engine.


Plan of next release.
-------

- API `/stream/jobs/kill` and `/stream/jobs/running`,`/run/sql` will be removed in next release.
- Module `streamingpro-automl`  will be removed in next release.

Docs Link
--------

- [MLSQL-1.2.0](http://docs.mlsql.tech/v1.2.0/zh/) 

Download Link
---------

- [MLSQL-1.2.0](http://download.mlsql.tech/1.2.0/)
   





