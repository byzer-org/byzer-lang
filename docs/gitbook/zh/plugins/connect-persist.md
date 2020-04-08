# Connect语句持久化

【文档更新日志：2020-04-07】

> Note: 本文档适用于MLSQL Engine 1.6.0-SNAPSHOT/1.6.0 及以上版本。  
> 对应的Spark版本应该为2.4.5,不低于2.4.3。

## 作用

该插件用于持久化connect语句。当系统重启后，无需再执行connect语句。

## 安装

> 如果MLSQL Meta Store 采用了MySQL存储，那么你需要使用 https://github.com/allwefantasy/mlsql-plugins/blob/master/connect-persist/db.sql
> 中的表创建到该MySQL存储中。

完成如上操作之后，安装插件：

```
!plugin app add - 'connect-persist-app-2.4';
```


## 使用示例

```sql
!connectPersist;
```

所有执行过的connect语句都会被保留下来。当系统重启后，会重新执行。

## 项目地址

[connect-persist](https://github.com/allwefantasy/mlsql-plugins/tree/master/connect-persist)