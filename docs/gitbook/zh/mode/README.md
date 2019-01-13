# MLSQL两种运行模式

MLSQL 支持

1.Service(服务)模式
2.Application(应用)模式 

## Service(服务)模式

所谓服务模式，也就是常驻进程模式，多租户通过http交互的方式使用。这也是MLSQL-Engine最常用的
模式。

## Application(应用)模式

所谓应用模式，其实就是运行完立刻结束进程。这种和传统的Spark批处理模式是一致的。
