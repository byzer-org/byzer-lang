# Shell模式

shell模式就是在driver端执行shell命令，并且将该命令的结果返回给变量，方便后续使用。比如

```sql
set date=`date` where type="shell";
```

注意这里需要使用反引号，表明是可执行的。因为具有一定的危险性，该模式可以纳入auth体系。