# MLSQL插件商店 

> Note: 本文档适用于MLSQL Engine 1.6.0-SNAPSHOT/1.6.0 及以上版本。  
> 对应的Spark版本应该为2.4.5,不低于2.4.3。

MLSQL支持通过插件全面的扩展功能。事实上，MLSQL自身就完全是以内置插件的形式构建起来的。现在，MLSQL也提供了[插件商店](https://store.mlsql.tech/),
后续新开发的功能都会以插件形式提供出来。


MLSQL 支持四种类型的插件：

1. ET 插件
2. DataSource 插件
3. Script 插件
4. App 插件


如果从数据处理的角度而言，DataSource插件可以让你扩展MLSQL访问你想要的数据源，而ET插件则可以完成数据处理相关的工作，甚至构建一个机器学习集群，
比如TF Cluster 实际上就是一个ET插件。 Script 插件则是一个更高层次的插件，是可复用的MLSQL代码。


MLSQL安装插件的方式很简单，在MLSQL Console中执行以下指令。

```sql
!plugin et add - delta_enhancer named delta_enhancer;
```

这个指令表示我要增加一个et插件，插件名字叫 delta_enhancer， 这个ET插件我想以后通过`!delta_enhancer` 来使用。


> 为了使用该功能，你需要启动MLSQL Engine是设置 -streaming.datalake.path 参数，并确保运行MLSQL Engine的账号有权限读写该目录。
