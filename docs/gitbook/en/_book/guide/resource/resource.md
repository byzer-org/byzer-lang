# Add/Remove/Set resource at runtime

There are three commands to adjust your resource at runtime:

```sql
!resource add 10c;
!resource remove 10c;
!resource set 40c;
```

The fist line above means we will add 10 cores. 
The second line means we can reduce 10 cores.
The third means we wanna set the total cores  the MLSQL Engine taken to 40c.

So what about memory? MLSQL Engine will add or reduce memory according to the  rate of cpu cores and memory size at startup.
For example, if your rate is 1:4 which means 1 core with 4g memory, then you add 10c, MLSQL Engine
will also add 40g memory.

If you use MLSQL Console with auth enabled, you should be the team admin to operate this command. 