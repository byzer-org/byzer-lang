# Conf 

conf is equal to execute `spark.sql("set a=b")`. Let's take a look how to use conf:

```sql
set spark.sql.shuffle.partitions="200" where type="conf";
```

This only work for current user.