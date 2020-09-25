# 基本安装

> 目前经过测试的兼容版本如下：
> 1. Spark 2.4.3
> 2. Spark 3.0.0


该章节为 【MLSQL Engine】 多种部署方式说明。 MLSQL Engine复用了Spark的部署工具，使用spark-submit命令进行部署。

这意味着：

1. 需要有Spark发行包
2. 支持Spark支持的所有环境

MLSQL Engine 的发行包可以在 [MLSQL官方下载站点](http://download.mlsql.tech/)知道。目前最新版本为[2.0.0](http://download.mlsql.tech/2.0.0/).

在对应版本的下载目录里，你应该会看到两个发行包:

1. mlsql-engine_2.4-2.0.0.tar.gz      
2. mlsql-engine_3.0-2.0.0.tar.gz 


我们以第一个为例，解释包名的含义：

1. 2.4以及3.0 都表示依赖的Spark版本
2. 2.0.0 表示MLSQL Engine的自身的版本


