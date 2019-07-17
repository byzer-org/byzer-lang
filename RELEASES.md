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
   





