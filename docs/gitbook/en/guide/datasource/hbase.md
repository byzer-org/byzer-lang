# HBase

The jar of hbase driver is not included by default. If you want to use this datasource, please 
make sure you have added the jar with `--jars` in startup command.

You can get the jar like this:

```
git clone  https://github.com/allwefantasy/streamingpro .
mvn -Pshade -am -pl external/streamingpro-hbase -Pspark-2.4.0 -Pscala-2.11 -Ponline clean package
```

Add the `external/streamingpro-hbase/target/streamingpro-hbase-x.x.x-SNAPSHOT.jar` with `--jars`

## Load data

Example：

```sql
connect hbase where `zk`="127.0.0.1:2181"
and `family`="cf" as hbase1;

load hbase.`hbase1:mlsql_example`
as mlsql_example;

select * from mlsql_example as show_data;


select '2' as rowkey, 'insert test data' as name as insert_table;

save insert_table as hbase.`hbase1:mlsql_example`;
```

Notice the splitter in HBase is ":".

