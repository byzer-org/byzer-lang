# ElasticSearch

ElasticSearch 是一个应用很广泛的数据系统。MLSQL也支持将其中的某个索引加载为表。

注意，ES的包并没有包含在MLSQL默认发行包里，所以你需要通过 --jars 带上相关的依赖才能使用。

## 加载数据

示例：

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

在ES里，数据连接引用和表之间的分隔符不是`.`,而是`/`。 这是因为ES索引名允许带"."。
所以es相关的参数可以参考驱动[官方文档](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html)。

