## SQL批处理计算


StreamingPro目前主流是以Server 方式运行，所以在按本文档进行操作之前，
你需要先把[运行StreamingPro Server](https://github.com/allwefantasy/streamingpro/blob/master/docs/run-server.md))。

如果你对XQL语法不熟悉，请参看[XQL语法](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-grammar.md))。



## 使用

一旦你启动了StreamingPro Server,你就可以提交SQL脚本，并且让系统执行了。

本例随便造一些数据，然后保存成json格式。

```sql
select "a" as a,"b" as b
as abc;

-- 这里只是表名你是可以使用上面的表形成一张新表
select * from abc
as newabc;

save overwrite newabc
as csv.`/tmp/abc` options header="true" 
```

建议使用Postman之类的交互工具。










