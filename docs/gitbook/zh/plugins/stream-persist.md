# 流程序持久化

【文档更新日志：2020-04-07】

> Note: 本文档适用于MLSQL Engine 1.6.0-SNAPSHOT/1.6.0 及以上版本。  
> 对应的Spark版本应该为2.4.5,不低于2.4.3。

## 作用

该插件用于持久化流程序。当系统重启后，会自动启动之前启动过的流。

## 安装

> 如果MLSQL Meta Store 采用了MySQL存储，那么你需要使用 https://github.com/allwefantasy/mlsql-plugins/blob/master/stream-persist/db.sql
> 中的表创建到该MySQL存储中。

完成如上操作之后，安装插件：

```
!plugin app add - "stream-persist-app-2.4";
```


## 使用示例

```sql
!streamPersist persist streamExample;

!streamPersist remove streamExample;

!streamPersist list;
```

指定的流程序会被保留下来。当系统重启后，会重新执行。

## 项目地址

[stream-persist](https://github.com/allwefantasy/mlsql-plugins/tree/master/stream-persist)