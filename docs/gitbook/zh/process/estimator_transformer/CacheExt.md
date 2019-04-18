# 如何缓存表

缓存表的场景是存在的，比如我们要把一张表存储到多个数据源，或者说我希望缓存一张表给流计算join,这个时候就可以把表缓存住。

具体使用方法如下：

```sql
!cache tableName script;
```

这句话表示我们会缓存tableName，并且该缓存只会在脚本期内生效。第二参数可选值有：

```
script      脚本运行结束自动清理缓存 
session     还没有实现  
application 需要手动释放
```

手动清理缓存也比较简单

```sql
!uncache tableName;
```