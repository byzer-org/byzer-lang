# MLSQL Cluster 路由策略

## 前言
MLSQL Cluster 具备多MLSQL Engine 实例管理功能，实现负载均衡，多业务切分等等功能。

## 负载均衡

MLSQL Cluster 有一个和MLSQL Engine完全一致的 /run/script 接口，参数也是保持一致的。

> 如何查看该接口详细参数以及MLSQL
Cluster 有什么接口，可以参看 [MLSQL初学者常见问题QA（持续更新）](https://www.jianshu.com/p/3adbf19bec65) 中【哪里有MLSQL三套件的http接口文档】部分内容。
我们可以把多个实例打上同一个标签，这样就可以实现负载均衡了。

在MLSQL Engine的基础上，多出了两个参数：

```
tags 
proxyStrategy
```

tags决定访问哪些engine, proxyStrategy 决定如何访问这些Engine. proxyStrategy 的可选参数有：

1.  ResourceAwareStrategy 资源剩余最多的Engine将获得本次请求
2. JobNumAwareStrategy   任务数最少的Engine将获得本次请求
3.  AllBackendsStrategy     所有节都将获得本次请求

默认是ResourceAwareStrategy策略。

一个简单的请求如下：

```sql
curl -XPOST http://127.0.0.1:8080 -d 'sql=....& tags=...& proxyStrategy=JobNumAwareStrategy'
``` 