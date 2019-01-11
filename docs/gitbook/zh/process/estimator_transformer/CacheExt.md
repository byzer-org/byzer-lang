# 如何缓存表

缓存表的场景是存在的，比如我们要把一张表存储到多个数据源，或者说我希望缓存一张表给流计算join,这个时候就可以把表缓存住。

具体使用方法如下：

```sql
run table1 CacheExt.`` where execute="cache" and isEager="true";
```

其中isEager是指立马缓存么？ 否则只有后面出发一次，第二次才使用缓存结果。

一定要注意的是，cache是需要手动释放的，释放方法如下：


```sql
run table1 CacheExt.`` where execute="uncache";
```