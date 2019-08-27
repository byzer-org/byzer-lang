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