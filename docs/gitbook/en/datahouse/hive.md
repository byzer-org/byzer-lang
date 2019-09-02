#Load Hive table and save

It is simple for MLSQL to use Hive, just needs one command:
```sql
load hive.`db1.table1`  as table1;
```

To save table:
```sql
save overwrite table1 as hive.`db.table1`;
```

If you need partitions:
```
save overwrite table1 as hive.`db.table1` partitionby col1;
```

You can also use JDBC to access hive:
```sql
load jdbc.`db1.table1` 
where url="jdbc:hive2://127.0.0.1:10000"
and driver="org.apache.hadoop.hive.jdbc.HiveDriver"
and user="" 
and password="" 
and fetchsize="100";
```



