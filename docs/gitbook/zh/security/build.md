# 如何开发自定义授权规则

##概要

MLSQL 里权限控制的基本单元是表。
MLSQL 支持如下对表操作类型的权限控制
        *  create
        *  drop
        *  load
        *  save
        *  select
        *  insert 
其中，我们对`load`的`jdbc`载入方式细化了数据源类型，如`mysql、postgresql`等等；

MLSQL的权限控制逻辑为：

1. 解析MLSQL脚本，然后返回MLSQL 脚本涉及到的所有表相关信息
2. 获取用户配置的权限中心接口
3. 调用接口得到权限中心的返回的内容
4. 调用用户配置的权限控制实现，实现对权限的拦截。

其中MLSQL解析脚本后返回的表的所有信息，对应的数据结构如下：

```
MLSQLTable(db, table, operateType, sourceType, tableType)


db：                数据库名称（es、solr是index名称、hbase为namespace名称、hdfs为None）
table：             表名称（es、solr是type名称、mongo为集合、hdfs为全路径）
operateType：       create、drop、load、save、select、insert
sourceType：        hbase、es、solr、mongo、jdbc（mysql、postgresql）、
                    hdfs（parquet、json、csv、image、text、xml）
tableType：         table的元数据类型
```

## 如何启用授权

1. `MLSQL HTTP API`请求参数增加`skipAuth=false`，默认true不开启；
2. `MLSQL HTTP API`请求参数auth client（`DefaultConsoleClient`是测试授权demo）和auth url（需要自己实现）设置，如下：

```
context.__default__auth_client__="streaming.dsl.auth.meta.client.MyConsoleClient"
context.__default__auth_url__="http://ip:port/rightCheck"
```
3. auth client和auth url也可以在`submit`的时候配置，如下：

```
-context.__default__auth_url__ streaming.dsl.auth.meta.client.MyConsoleClient
-context.__default__auth_url__ http://ip:port/rightCheck
```

##示例：
####1. 执行sql
```
load parquet.`/tmp/abc` as newtable;
select * from default.abc as cool;

load jdbc.`test.people` options
driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/test"
and user="root"
and password="root"
as people;
```

####2. 授权List
```
[owner] [william] auth 

MLSQLTable(None,Some(/tmp/abc),load,parquet,TableTypeMeta(hdfs,Set(parquet, json, csv, image)))

MLSQLTable(None,Some(newtable),load, None,TableTypeMeta(temp,Set(temp, jsonStr, script)))

MLSQLTable(Some(default),Some(abc),select, None,TableTypeMeta(hive,Set(hive)))

MLSQLTable(None,Some(cool),select, None,TableTypeMeta(temp,Set(temp, jsonStr, script)))

MLSQLTable(Some(test),Some(people),load,mysql,TableTypeMeta(jdbc,Set(jdbc)))

MLSQLTable(None,Some(people),load, None,TableTypeMeta(temp,Set(temp, jsonStr, script)))

说明：
操作类型load，数据源hdfs，路径为`/tmp/abc`
由load注册的临时表newtable
操作类型select，数据库为default，表名称为abc
由select注册的临时表cool
操作类型load，数据源mysql，数据库为test，表名称为people
由load注册的临时表people
```

####3. 自定义授权

######实现模块`streamingpro-api`下的`streaming.dsl.auth.TableAuth`接口`def auth(table: List[MLSQLTable]）`，默认client位置为`streaming.dsl.auth.meta.client`，示例如下：
```
1、构造授权json请求（ {"hdfs":{"parquet":{"load":["/tmp/abc","/tmp/efg"]}}}）
val authJsonStr = constructAuthJsonStr(table)

2、获取授权服务地址（优先取set的__auth_url__，否则取submit时候传入的context.__default__auth_url__）
val authUrl = ScriptSQLExec.context().userDefinedParam.getOrElse("__auth_url__"
          ,Dispatcher.contextParams("").getOrDefault("context.__default__auth_url__" ,"http://ip:port/rightCheck/rightCheck/").toString)

3、请求授权服务
val htp = ServiceFramwork.injector.getInstance(classOf[HttpTransportService])
val resp = htp.get(new Url(authUrl), Map("data" -> authJsonStr, "userId" -> owner))

4、解析授权返回结果，判断是否授权成功
val respJson = JSONObject.fromObject(resp.getContent)

5、auth返回结果
如果授权失败抛出运行时异常 throw new RuntimeException("william request auth select default.abc failed!")
否则返回 List(TableAuthResult(true ,""))

```