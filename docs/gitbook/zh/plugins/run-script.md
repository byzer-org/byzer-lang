# 将字符串当做代码执行

【文档更新日志：2020-04-07】

> Note: 本文档适用于MLSQL Engine 1.6.0-SNAPSHOT/1.6.0 及以上版本。  
> 对应的Spark版本应该为2.4.5,不低于2.4.3。

## 作用

该插件用于将字符串当做MLSQL脚本执行。

## 安装

> 如果MLSQL Meta Store 采用了MySQL存储，那么你需要使用 https://github.com/allwefantasy/mlsql-plugins/blob/master/stream-persist/db.sql
> 中的表创建到该MySQL存储中。

完成如上操作之后，安装插件：

```
!plugin et add - "run-script-2.4" named runScript;
```


## 使用示例

```sql
set code1='''
select 1 as a as b;
''';
!runScript '''${code1}''' named output;
```


## 项目地址

[run-script](https://github.com/allwefantasy/mlsql-plugins/tree/master/run-script)