# How to cache table

Cache table is really useful.

You can use the macro of `!cache`:

```sql
!cache tableName script;
```

the second parameters have three enums:

```
script      script lifetime , once the script is finished, the cache will be cleaned.
session     not support yet.  
application the cache will valid until MLSQL Service is shutdown. you should uncache it manually.
```

You can uncache table manually with the follow command:

```sql
!uncache tableName;
```