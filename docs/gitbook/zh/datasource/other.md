## 其他

对于任何MLSQL没有适配过的数据源，只要他是符合Spark datasource 标准API的，其实也是可以被集成的。具体做法如下：

```
load unknow.`` where implClass="数据源完整包名" as table;
save table as unknow.`/tmp/...` where implClass="数据源完整包名";
```

其中 unknow这个词汇是可以任意的，因为我们真正在意的是implClass里配置的完整包名。
如果你该驱动有什么其他参数，你可以放在where 中。

