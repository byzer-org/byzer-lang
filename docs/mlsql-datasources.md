## MLSQL对常见数据源的加载和保存

### kafka 0.8/0.9

```sql
load kafka9.`` options `kafka.bootstrap.servers`="127.0.0.1:9092"
and `topics`="testM"
as newkafkatable1;

select CAST(key AS STRING),CAST(value AS STRING) from newkafkatable1
as resultTable;
```

通常我们只会用到value字段。在kafka 0.8/0.9 实现中还有一些其他字段。

```
def kafkaSchema: StructType = StructType(Seq(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType)
  ))
```

如果希望保存，则如下：

```sql
select * from table 
as newtable;

save append newtable as kafka8.`-` options 
`kafka.bootstrap.servers`="host1:port1,host2:port2"
topic="topic1"
```


### kafka 1.0 


```sql
load kafka.`` options `kafka.bootstrap.servers`="127.0.0.1:9092"
and `subscribe`="testM"
as newkafkatable1;

select CAST(key AS STRING),CAST(value AS STRING) from newkafkatable1
as resultTable;
```
 
除了key,value,kafka 1.0 里还有一些其他字段:

|Column	|Type|
|:---|:---|
|key | binary |
|value|	binary|
|topic|	string|
|partition|	int|
|offset|	long|
|timestamp|	long|
|timestampType|	int|

如果希望保存，则使用：

如果希望保存，则如下：

```sql
select * from table 
as newtable;

save append newtable as kafka.`-` options 
`kafka.bootstrap.servers`="host1:port1,host2:port2"
topic="topic1"
```

### ElasticSearch

读取：

```sql
load es.`index/type` options `es.nodes`="127.0.0.1:9200"
ittable;
```

当然，你也可以使用connect语法：

```sql
connect es where `es.nodes`="127.0.0.1:9200"
as es1;

load es.`es1.index/type` 
as ittable;

select * from ittable;
```

保存则是类似的。

### MySQL

读取一张表并且写入另外一张表：

```sql

load jdbc.`wow.abc`
options 
and driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/...."
and driver="com.mysql.jdbc.Driver"
and user="..."
and password="...."
as table1;

save overwrite table1
as jdbc.`tableau.abc`
options truncate="true"
and driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/...."
and driver="com.mysql.jdbc.Driver"
and user="..."
and password="...."
```

当然，每次写load/save语句，都要写那么多配置选项，会很痛苦，可以使用connect语法：


```sql
connect jdbc where driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/wow?..."
and driver="com.mysql.jdbc.Driver"
and user="..."
and password="...."
as tableau;

```

这样就相当于你拥有了一个叫tableau数据库的引用了,当然，真实的数据库名叫wow。这样前面的语句就可以简化为：

```sql
load jdbc.`tableau.abc` 
as jacktable;

save overwrite jacktable
as jdbc.`tableau.abc-copy`
options truncate="true"
```

我们知道，Spark JDBC是没办法支持upsert 的，也就是我们说的，如果记录存在则更新，否则则新增的语义。StreamingPro对此提供了支持：

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

## Carbondata

```sql
select "1" as a
as mtf1;

-- 如果需要指定db名称，则使用类似 adb.visit_carbon 这种
save overwrite mtf1
as carbondata.`visit_carbon`
```

如果是流式写入Carbondata,参看：[Carbondata流式写入](https://github.com/allwefantasy/streamingpro/blob/master/docs/carbondata.md)

## Hive

使用方式和形态和Carbondata一样。



### 普通文件

```sql
load parquet.`path` 
as table1;

load csv.`path` options header="true"  
as table2;

save append tableName as csv.`path` optioins header="true";
```

### HBase 

读取HBase:

```sql
load hbase.`wq_tmp_test1` 
options rowkey="id"
and zk="---"
and inputTableName="wq_tmp_test1"
and field.type.filed1="FloatType"
and field.type.filed2="TimestampType"
and field.type.filed3="BinaryType"
as testhbase 
;
```

读取的过程中需要指定rowkey以及zookeeper地址。 field.type.`fieldname` 则可以指定字段的类型。在HBase中，你是无法获悉某个属性的类型的。
其中fieldname 需要是`family:column`的格式。

写入HBase也比较简单：


```sql
select "a" as id, "b" as ck, 1 as jk
as      
wq_tmp_test2
;

save overwrite wq_tmp_test2 
as hbase.`wq_tmp_test1`  options  rowkey="id" and family="f";
```

目前只支持一次写入一种列族。
如果要使用HBase相关的读写功能，用户需要额外打包streamingpro-hbase 子模块。之后在启动StreamingPro Server 的时候 用--jars 带上。

### Redis

Redis 属于没有schema概念的东西，所以比较特殊。只实现了部分功能：

```sql
select "a" as id, "b" as ck, bin(1) as jk
as      
wq_tmp_test2
;
save overwrite wq_tmp_test2 
as redis.`wq_tmp_test1`  options  insertType="listInsert" and host="127.0.0.1" and port="6379";
```

insertType=listInsert 代表会简单的把wq_tmp_test2的是数据的表的第一列字段拼接成一个集合然后按wq_tmp_test1 作为key写入到redis。

如果要使用Redis相关的读写功能，用户需要额外打包streamingpro-redis 子模块。之后在启动StreamingPro Server 的时候 用--jars 带上。

```sql
select "a" as id, split("b,a,c",",") as ck
as      
wq_tmp_test2
;
save overwrite wq_tmp_test2 
as redis.`wq_tmp_test1`  options  insertType="KVList" and host="127.0.0.1" and port="6379" and join="---";
```

当insertType为KVList时，第一列类型为String,第二列了类型为List(比如你使用了collect_list之类的函数)，然后第一列作为Key,第二列会为Value,并且默认使用","进行拼接成字符串。

如果Redis使用了sentinelRedis，则应该按如下方式使用：

```
select "a" as id, "b" as ck, bin(1) as jk
as      
wq_tmp_test2
;
save overwrite wq_tmp_test2 
as redis.`wq_tmp_test2`  options  
insertType="listInsertAsString" 
and host="127.0.0.1" 
and port="6379" 
and dbNum="0" 
and sentinelRedisEnable="True" 
and "master_name"="wow"
```








