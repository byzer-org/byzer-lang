# MLSQL 编译时权限控制

## 前言

权限控制，对于MLSQL而言的重要程度可以说是生命线。 MLSQL需要面对各式各样的资源访问，比如MySQL, Oracle,HDFS，
Hive,Kafka,Sorl,ElasticSearch,Redis,API,Web等等，不同用户对这些数据源（以及表，列）的权限是不一样的。

传统模式是，每个用户都需要有个proxy user,然后到每个数据源里面给这个proxy user进行授权。 这看起来似乎就是麻烦点，但是在实际操作中，基本是很难执行的，不同的数据源在不同的团队里面，那么整个申请流程可能要天甚至周计了。

如果上面的问题已经让人气馁，那么对于采用Hive做数仓的公司，可能对HIve权限访问更让人绝望。Hive的授权模式是跟着Linux用户走的，也就是Spark启动用户是谁，谁就有权限访问，这个对于多租户的MLSQL应用来说，则是完全不可行了，比如启动Spark的是sparkUser,但是真正执行的人，其实可能是张三，李四等等。Hive就无法知道是具体哪个人完成的，只知道是sparkUser。

还有一个大家可能感慨的点：

> 我们好不容易写了个脚本，跑了一个小时，突然脚本失败，一看，第350行那里访问的数据源权限不足。 这可真是让人恼火。

## 问题来了

那么，怎么才能在脚本运行前，就知道脚本里涉及到的资源是不是都被授权了？

## 答案是：有

> 题外话：标题不严谨，因为MLSQL本质是个解释性执行语言，不需要编译，更好的标题是 【解析时权限控制】。

MLSQL如果开启了权限验证，他会先扫描整个脚本，然后提取必要的信息，这些信息就包含了各种数据源的详细信息，从而在运行前就可以知道你是不是访问了未经授权的库表。那么MLSQL是怎么做到的呢？我们来看下面的信息：

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

MLSQL在解析load语句的时候，会询问当前用户访问的表，有哪些列是被授权的，然后会改写最后load的语句，提供一个新的视图，该视图只有用户被授权的列。

## 总结

MLSQL通过一些有效的限制，可以在语法解析层面直接提取了所有数据源相关信息，并且将其发送给到配套的权限中心进行判断，避免在运行时发现授权拒绝问题。MLSQL此举意义重大，使得MLSQL系统不再完全依赖于底层系统的权限控制，从而让问题得到了极大的简化。