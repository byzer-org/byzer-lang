# ElasticSearch

The jar of elasticsearch-hadoop is not included by default. If you want to use this datasource, please 
make sure you have added the jar with `--jars` in startup command.  


## load data

Example：

```sql
set data='''
{"jack":"cool"}
''';

load jsonStr.`data` as data1;

save overwrite data1 as es.`twitter/cool` where
`es.index.auto.create`="true"
and es.nodes="127.0.0.1";

load es.`twitter/cool` where
and es.nodes="127.0.0.1"
as table1;
select * from table1 as output1;

connect es where  `es.index.auto.create`="true"
and es.nodes="127.0.0.1" as es_instance;

load es.`es_instance/twitter/cool`
as table1;
select * from table1 as output2;
```


Notce that the splitter in ES is "/" not ".".
More detail about  es driver:[官方文档](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html)。

