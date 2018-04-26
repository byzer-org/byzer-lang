## 如何通过XQL操作MySQL数据库

StreamingPro支持标准的Spark 操作JDBC的方式。比如：

```sql
select "a" as a,"b" as b
as abc;

save overwrite abc
as jdbc.`tableau.abc`
options truncate="true"
and driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/...."
and driver="com.mysql.jdbc.Driver"
and user="..."
and password="...."
```

当然，每次写save语句，都要写那么多配置选项，会很痛苦，StreamingPro额外支持connect语法：


```sql
connect jdbc where driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/wow?..."
and driver="com.mysql.jdbc.Driver"
and user="..."
and password="...."
as tableau;

```

这样就相当于你拥有了一个叫tableau数据库的引用了,当然，真实的数据库名叫wow。接着你就可以这么写save语句了：

```sql
select "a" as a,"b" as b
as abc;

save overwrite abc
as jdbc.`tableau.abc`
options truncate="true"
```

我们知道，Spark JDBC是没办法支持upsert 的，也就是我们是说，如果记录存在则更新，否则则新增的语义。StreamingPro对此提供了支持：

```sql
select "a" as a,"b" as b
as abc;

save append abc
as jdbc.`tableau.abc`
options truncate="true"
and idCol="a,b"
and createTableColumnTypes="a VARCHAR(128),b VARCHAR(128)";

load jdbc.`tableau.abc` as tbs;
```

其中，idCol表示哪些属性作为是作为主键的。因为比如text字段是无法作为主键，所以你需要指定原先类型为String的字段的类型，比如我这里吧a,b
两个字段的类型通过createTableColumnTypes修改为varchar(128)。

一旦StreamingPro发现了idCol配置，如果发现表没有创建，就会创建表，并且根据主键是否重复，来决定是进行更新还是写入操作。

如果你想访问一张mysql表，那么使用load语法即可；

```
load jdbc.`tableau.abc` as tbs;
select * from tbs;
```