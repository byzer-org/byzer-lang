# 基本的格式以及术语解释

【文档更新日志：2020-04-10】

> Note: 本文档适用于MLSQL Engine 1.3.0 及以上版本。  
> 对应的Spark版本可支持2.3.2/2.4.3

正如前面我们讨论的，MLSQL支持表以及列级别的权限。对于表而言，有如下几种操作权限：

*  create
*  drop
*  load
*  save
*  select
*  insert

其中MLSQL解析脚本后返回的表的所有信息，对应的数据结构如下：

```
MLSQLTable(
                       db: Option[String],
                       table: Option[String],
                       columns: Option[Set[String]],
                       operateType: OperateType,
                       sourceType: Option[String],
                       tableType: TableTypeMeta)
```

其中对应的字段名称为：


1. db：                数据库名称（es、solr是index名称、hbase为namespace名称、hdfs为None）
2. table：             表名称（es、solr是type名称、mongo为集合、hdfs为全路径）
3. operateType：       create、drop、load、save、select、insert
4. sourceType：        hbase、es、solr、mongo、jdbc（mysql、postgresql）、hdfs（parquet、json、csv、image、text、xml）
5. tableType：         table的元数据类型

如果只是表权限，那么columns里的值会为None.如果是到列的值，
则columns会得到填充。 通常而言，表权限会在解析MLSQL得到验证，而列级别的，只能在实际执行对应的语句时才会触发。

也就是说，MLSQL会提供MLSQLTable给你自定义的client,然后client会将这些信息发送给权限服务器，
然后根据返回结果告诉用户哪些表不具备权限从而终止脚本的执行。


##  示例

```sql
load parquet.`/tmp/abc` as newtable;
select * from default.abc as cool;

load jdbc.`test.people` options
driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/test"
and user="root"
and password="root"
as people;
```

对于这段脚本，client 会得到一个MLSQLTable List,包含如下内容：

```scala

MLSQLTable(None,Some(/tmp/abc),load,parquet,TableTypeMeta(hdfs,Set(parquet, json, csv, image)))

MLSQLTable(None,Some(newtable),load, None,TableTypeMeta(temp,Set(temp, jsonStr, script)))

MLSQLTable(Some(default),Some(abc),select, None,TableTypeMeta(hive,Set(hive)))

MLSQLTable(None,Some(cool),select, None,TableTypeMeta(temp,Set(temp, jsonStr, script)))

MLSQLTable(Some(test),Some(people),load,mysql,TableTypeMeta(jdbc,Set(jdbc)))

MLSQLTable(None,Some(people),load, None,TableTypeMeta(temp,Set(temp, jsonStr, script)))

```

说明：

1. 操作类型load，数据源hdfs，路径为`/tmp/abc`
2. 由load注册的临时表newtable
3. 操作类型select，数据库为default，表名称为abc
4. 由select注册的临时表cool
5. 操作类型load，数据源mysql，数据库为test，表名称为people
6. 由load注册的临时表people