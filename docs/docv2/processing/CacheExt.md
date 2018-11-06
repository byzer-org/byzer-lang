
```sql
select 1 as a,'jack' as b2 as bbc;

--cache table
run bbc as CacheExt.`` where execute="cache";

select * from bbc as outoutp;

-- uncache table
run bbc as CacheExt.`` where execute="uncache";
```