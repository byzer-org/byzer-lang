# Conf模式

conf模式相当于执行`spark.sql("set a=b")`. 我们看看具体如何使用：

```
set spark.sql.shuffle.partitions="200" where type="conf"; 
```

该配置会对当前用户生效。由上面的例子可知，该模式主要是为了配置底层spark引擎用的。