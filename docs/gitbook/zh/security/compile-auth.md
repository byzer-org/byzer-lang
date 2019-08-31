# MLSQL 编译时权限控制



MLSQL如果开启了权限验证，他会先扫描整个脚本，然后提取必要的信息，这些信息就包含了各种数据源的详细信息，
从而在运行前就可以知道你是不是访问了未经授权的库表。那么MLSQL是怎么做到的呢？我们来看下面的信息：

```
connect jdbc where
driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://${ip}:${host}/db1?${MYSQL_URL_PARAMS}"
and user="${user}"
and password="${password}"
as db1_ref;

load jdbc.`db1_ref .people`
as people;

save append people as jdbc.`db1_ref.spam` ;
```
因为MLSQL要求任何数据源，都需要使用load语句进行加载，在解析load语句时，MLSQL知道，用户现在要访问的是基于JDBC协议的数据源访问，他通过url拿到了这些信息：

```
db: db1
table: people
operateType: load
sourceType: mysql
tableType: JDBC
```
当然，这个脚本用户还会写入一张spam表，也一样会被提取信息：

```
db: db1
table: people
operateType: save
sourceType: mysql
tableType: JDBC
```

然后还有一张临时表people,所以这个脚本总共有三张表信息，之后这些信息会被发送到AuthCenter里进行判断，AuthCenter会告诉MLSQL那张表是没有对当前用户授权的，如果发现未经授权的表，MLSQL会直接抛出异常。整个过程中，完全不会执行任何物理计划，只是对脚本的信息抽取。

在MLSQL中，我们不能在select语句里访问hive表，只能通过load语句加载，比如下面的句子会报错：

```
select * from public.abc as table1;
```

我们无权在select语句中访问public.abc库，如果需要使用，你可以通过如下方式完成：

```
load hive.`public.abc ` as abc;
select * from abc as table1;
```

## 如何实现列级别控制

MLSQL在解析load语句的时候，会询问当前用户访问的表，有哪些列是被授权的，然后会改写最后load的语句，
提供一个新的视图，该视图只有用户被授权的列。

## 总结

MLSQL通过一些有效的限制，可以在语法解析层面直接提取了所有数据源相关信息，并且将其发送给到配套的权限中心进行判断，
避免在运行时发现授权拒绝问题。MLSQL此举意义重大，使得MLSQL系统不再完全依赖于底层系统的权限控制，从而让问题得到了极大的简化。