# 运行时添加数据源依赖

我们启动MLSQL-Engine之后，会发现，哦，忘了添加MongoDB依赖了。这个时候无法通过MLSQL语言访问MongoDB。
MLSQL支持数据源中心的概念，可以动态添加需要的依赖。

通过如下脚本查看目前支持的可动态添加的数据源：

```sql
run command as DataSourceExt.`` 
where command="list" 
and sparkV="2.4" 
and scalaV="2.11"
as datasource;
```

用户需要指定spark的版本以及scala的版本。目前sparkV有两个可选值：2.3/2.4, scalaV为 2.11.

接着我们根据名字找到所有可用版本，这里我们以ES为例子：

```sql
run command as DataSourceExt.`es` 
where command="version" and sparkV="2.4" and scalaV="2.11"
as datasource;
```


我们选择最新的7.3.0版本的ES,然后按如下指令进行动态添加：

```sql
run command as DataSourceExt.`es/7.3.0` 
where command="add" and sparkV="2.4" and scalaV="2.11"
as datasource;
```

添加完毕后会显示如下结果：

```
desc

Datasource is loading jar from ./dataousrce_upjars/elasticsearch-spark-20_2.11-7.3.0.jar"]
```

现在，你可以使用最新的ES驱动加载和存储ES了。

我们后续会努力添加更多的数据源支持。


