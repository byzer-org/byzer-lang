## 直接操作MySQL

通过前面介绍的jdbc数据源，其实我们可以完成对MySQL数据的读取和写入。但是如果我们希望删除或者创建表呢？这个时候可以使用
JDBC Transformer.具体用法如下：

```sql
set user="root";
 
 connect jdbc where
 url="jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
 and driver="com.mysql.jdbc.Driver"
 and user="${user}"
 and password="${password}"
 as db_1;


select 1 as a as FAKE_TABLE;

run FAKE_TABLE as JDBC.`db_1` where 
driver-statement-0="drop table test1"
and driver-statement-1="create table test1.....";
```

我们先用connect语法获得数据连接，然后通过JDBC transformer完成删除和创建表的工作。 driver-statement-[number]  中的number表示执行的顺序。
这里有个技巧，其实JDBC并不是做数据处理用的，所以其实没有数据需要他处理，这个时候因为语法要求，我们还需要传入一张表。我们通过如下方式创建了一张FAKE_TABLE
方便后续占位使用：

```sql
select 1 as a as FAKE_TABLE;
```

在后面很多场景，我们都会遇到这个技巧。