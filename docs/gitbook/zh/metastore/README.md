##  MLSQL元信息存储

MLSQL会存储一些信息，比如已经安装的plugin, scheduler service 的任务等等。目前MLSQL提供了
两种存储的支持：

1. Delta Lake
2. MySQL

默认是delta lake. 开启方式为：

```
-streaming.datalake.path [HDFS路径]
```

也可以替换成 MySQL,开启方式为：

```
-streaming.metastore.db.type  "mysql",
-streaming.metastore.db.name  "app_runtime_full",
-streaming.metastore.db.config.path "./__mlsql__/db.yml"
```

你需要创建一个数据库，然后将项目根目录下的db.sql导入进去。db.yml的示例配置如下：

```
app_runtime_full:
  host: 127.0.0.1
  port: 3306
  database: app_runtime_full
  username: xxxxx
  password: xxxx
  initialSize: 8
  disable: false
  removeAbandoned: true
  testWhileIdle: true
  removeAbandonedTimeout: 30
  maxWait: 100
  filters: stat,log4j
```
