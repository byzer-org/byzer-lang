# 如何缓存表

Spark有一个很酷的功能，就是cache,允许你把计算结果分布式缓存起来，但存在需要手动释放的问题。
MLSQL认为要解决这个问题，需要将缓存的生命周期进行划分：

1. script
2. session
3. application

默认缓存的生命周期是script。随着业务复杂度提高，一个脚本其实会比较复杂，在脚本中我们存在反复使用原始表或者中间表临时表的情况，这个时候我们可以通过cache实现原始表被缓存，中间表只需计算一次，然后脚本一旦执行完毕，就会自动释放。使用方式也极度简单：

```sql
select 1 as a as table1;
!cache table1 script;
select * from table1 as output;
```

session级别暂时还没有实现。application级别则是和MLSQL Engine的生命周期保持一致。需要手动释放：

```sql
!uncache table1;
```

表缓存功能极大的方便了用户使用cache。对于内存无法放下的数据，系统会自动将多出来的部分缓存到磁盘。