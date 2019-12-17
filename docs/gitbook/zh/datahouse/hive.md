#Hive加载和存储

Hive在MLSQL中使用极为简单。 加载Hive表只需要一句话：

```sql
load hive.`db1.table1`  as table1;
```

保存则是：

```sql
save overwrite table1 as hive.`db.table1`;
```

如果需要分区，则使用

```
save overwrite table1 as hive.`db.table1` partitionby col1;
```

我们也可以使用JDBC访问hive,具体做法如下：

```sql
load jdbc.`db1.table1` 
where url="jdbc:hive2://127.0.0.1:10000"
and driver="org.apache.hadoop.hive.jdbc.HiveDriver"
and user="" 
and password="" 
and fetchsize="100";
```  

我们也可以使用数据湖替换实际的hive存储：

1. 启动时配置 `-streaming.datalake.path` 参数,启用数据湖。
2. 配置 `-spark.mlsql.datalake.overwrite.hive` hive默认采用数据存储。

使用时如下：

```sql
set rawText='''
{"id":9,"content":"Spark好的语言1","label":0.0}
{"id":10,"content":"MLSQL是一个好的语言6","label":0.0}
{"id":12,"content":"MLSQL是一个好的语言7","label":0.0}
''';

load jsonStr.`rawText` as orginal_text_corpus;

select cast(id as String)  as rowkey,content,label from orginal_text_corpus as orginal_text_corpus1;
save overwrite orginal_text_corpus1 as hive.`public.orginal_text_corpus1`;

load hive.`public.orginal_text_corpus1` as output ;
```    

在你访问hive时，如果数据湖里没有，则会穿透数据湖，返回hive结果。如果你希望在写入的时候一定要写入到hive而不是数据湖里，可以这样：

```
save overwrite orginal_text_corpus1 as hive.`public.orginal_text_corpus1` where storage="hive"; 
```                                                                                             

强制指定存储为hive.