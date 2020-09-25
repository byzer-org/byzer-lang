# HBase

HBase 是一个应用很广泛的存储系统。MLSQL也支持将其中的某个索引加载为表。

注意，HBase的包并没有包含在MLSQL默认发行包里，所以你需要通过 --jars 带上相关的依赖才能使用。用户有三种种方式获得
HBase Jar包：

第一种，访问[spark-hbase](https://github.com/allwefantasy/spark-hbase),然后自己进行编译。

第二种，通过官网下载已经打包的好的。[地址](http://download.mlsql.tech/1.4.0-SNAPSHOT/mlsql-hbase/)

第三种(推荐)，直接使用Datasource插件：https://github.com/allwefantasy/mlsql-pluins/tree/master/ds-hbase-2x

你可以直接通过如下指令安装：

```sql
!plugin ds add tech.mlsql.plugins.ds.MLSQLHBase2x ds-hbase-2x;
```                                                           

因为HBase依赖很多，大概80多M,下载会比较慢。


MLSQL Code:

```sql
set rawText='''
{"id":9,"content":"Spark好的语言1","label":0.0}
{"id":10,"content":"MLSQL是一个好的语言7","label":0.0}
{"id":12,"content":"MLSQL是一个好的语言7","label":0.0}
''';

load jsonStr.`rawText` as orginal_text_corpus;

select cast(id as String)  as rowkey,content,label from orginal_text_corpus as orginal_text_corpus1;

connect hbase2x where `zk`="127.0.0.1:2181"
and `family`="cf" as hbase1;

save overwrite orginal_text_corpus1 
as hbase2x.`hbase1:mlsql_example`;

load hbase2x.`hbase1:mlsql_example` where field.type.label="DoubleType"
as mlsql_example ;

select * from mlsql_example as show_data;

```

DataFrame Code:

```scala
val data = (0 to 255).map { i =>
      HBaseRecord(i, "extra")
    }
val tableName = "t1"
val familyName = "c1"


import spark.implicits._
sc.parallelize(data).toDF.write
  .options(Map(
    "outputTableName" -> cat,
    "family" -> family
  ) ++ options)
  .format("org.apache.spark.sql.execution.datasources.hbase2x")
  .save()
  
val df = spark.read.format("org.apache.spark.sql.execution.datasources.hbase2x").options(
  Map(
    "inputTableName" -> tableName,
    "family" -> familyName,
    "field.type.col1" -> "BooleanType",
    "field.type.col2" -> "DoubleType",
    "field.type.col3" -> "FloatType",
    "field.type.col4" -> "IntegerType",
    "field.type.col5" -> "LongType",
    "field.type.col6" -> "ShortType",
    "field.type.col7" -> "StringType",
    "field.type.col8" -> "ByteType"
  )
).load() 
```         

一些有用的配置参数：

| Property Name  |  Meaning |
|---|---|
| tsSuffix |to overwrite hbase value's timestamp|
|namespace|hbase namespace|
| family |hbase family，family="" means load all existing families|
| field.type.ck | specify type for ck(field name),now supports:LongType、FloatType、DoubleType、IntegerType、BooleanType、BinaryType、TimestampType、DateType，default: StringType。|




