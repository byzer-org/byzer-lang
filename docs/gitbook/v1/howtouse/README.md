# 基本安装

该章节为 【MLSQL Engine】 多种部署方式说明。 MLSQL Engine复用了Spark的部署工具，使用spark-submit命令
进行部署。

这意味着：

1. 需要有Spark发行包
2. 支持Spark支持的所有环境

MLSQL Engine 的发型包可以在 [MLSQL官方下载站点](http://download.mlsql.tech/)知道。目前最新版本为[1.7.0-SNAPSHOT](http://download.mlsql.tech/1.7.0-SNAPSHOT/).

在对应版本的下载目录里，你应该会看到两个发型包:

1. mlsql-engine_2.4-1.7.0-SNAPSHOT.tar.gz      
2. mlsql-engine_3.0-1.7.0-SNAPSHOT.tar.gz 


我们以第一个为例，解释包名的含义：

1. 2.4以及3.0 都表示依赖的Spark版本
2. 1.7.0-SNAPSHOT 表示MLSQL Engine的自身的版本

MLSQL Engine 通常支持两个Spark版本。一个最新版本和一个稳定版本。在使用时，用户需要注意与之匹配的Spark 版本，否则会报错。
