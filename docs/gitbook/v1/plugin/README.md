# 插件

> 目前大部分还没有为 spark 3.0 做适配。

MLSQL支持插件机制。官方插件位于[MLSQL Plugins](https://github.com/allwefantasy/mlsql-plugins)。

插件分成四种类型：

1. ET 插件
2. DataSource 插件
3. Script 插件
4. App 插件

如果从数据处理的角度而言，DataSource插件可以让你扩展MLSQL访问你想要的数据源，而ET插件则可以完成数据处理相关的工作，甚至构建一个机器学习集群， 比如TF Cluster 实际上就是一个ET插件。 Script 插件则是一个更高层次的插件，是可复用的MLSQL代码。


> 为了使用该功能，你需要启动MLSQL Engine是设置 -streaming.datalake.path 参数，并确保运行MLSQL Engine的账号有权限读写该目录。
