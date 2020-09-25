# Solr

Solr 是一个应用很广泛的存储。MLSQL也支持将其中的某个索引加载为表。

注意，Solr的包并没有包含在MLSQL默认发行包里，所以你需要通过 --jars 带上相关的依赖才能使用。

## 加载数据

示例：

```sql
select 1 as id, "this is mlsql_example" as title_s as mlsql_example_data;

connect solr where `zkhost`="127.0.0.1:9983"
and `collection`="mlsql_example"
and `flatten_multivalued`="false"
as solr1
;

load solr.`solr1/mlsql_example` as mlsql_example;

save mlsql_example_data as solr.`solr1/mlsql_example`
options soft_commit_secs = "1";
```

在Solr里，数据连接引用和表之间的分隔符不是`.`,而是`/`。 这是因为Solr索引名允许带"."。
所有Solr相关的参数可以参考驱动[官方文档](https://github.com/lucidworks/spark-solr)。

