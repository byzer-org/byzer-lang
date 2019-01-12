# MLSQL简介

MLSQL不只是一门语言，也是一个分布式计算引擎。MLSQL可以实现批处理，流式处理，机器学习，爬虫，API服务等多领域功能。

MLSQL 目前由三部分构成：

1. MLSQL Engine
2. MLSQL Cluster
3. MLSQL Console

一个MLSQL Engine 由一个Driver 和多个Executor组成，它本身也是分布式的。MLSQL Cluster 实现了负载均衡，以及Engine管理相关的功能。
MLSQL Console 则提供了一个图形化界面方便用户使用MLSQL语言。 MLSQL Console可以对接MLSQL Engine 也可以对接MLSQL Cluster。

我们推荐使用如下结构：

```
MLSQL Console -> MLSQL Cluster -> MLSQL Engines
```

如果只是测试或者体验，则可以简化为：


```
MLSQL Console  -> MLSQL Engine
```
