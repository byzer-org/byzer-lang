# MLSQL Script插件 binlog2delta 开发示例 （1.5.0及以上版本支持）


MLSQL提供了 [插件商店](https://docs.mlsql.tech/zh/plugins/),方便开发者发布自己开发的插件。

MLSQL 支持四种类型的插件：

1. ET 插件
2. DataSource 插件
3. Script 插件
4. App 插件


如果从数据处理的角度而言，DataSource插件可以让你扩展MLSQL访问你想要的数据源，而ET插件则可以完成数据处理相关的工作，甚至构建一个机器学习集群，
比如TF Cluster 实际上就是一个ET插件。 Script 插件则是一个更高层次的插件，是可复用的MLSQL代码。

这篇文章，我们将重点介绍如何开发一个Script插件。


大家可以做在[github上找到项目的源码](https://github.com/allwefantasy/mlsql-pluins/tree/master/binlog2delta)。

## binlog2delta

binlog2delta 目的是让用户通过简单配置即可完成MySQL表到数据湖表的实时同步。可以通过如下方式安装：

```sql
!plugin script add - binlog2delta;
```

使用方式如下：

```sql
set streamName="binlog";

set host="127.0.0.1";
set port="3306";
set userName="root";
set password="mlsql";
set bingLogNamePrefix="mysql-bin";
set binlogIndex="1";
set binlogFileOffset="4";
set databaseNamePattern="mlsql_console";
set tableNamePattern="script_file";

set deltaTableHome="/tmp/binlog2delta";
set idCols="id";
set duration="10";
set checkpointLocation="/tmp/ck-binlog2delta";

include plugin.`binlog2delta`;
```

运行上面的MLSQL脚本，系统会启动一个流程序完成工作。

## 开发和打包

binlog2delta 目录结构如下：

```
-lib/
-main.mlsql
-plugin.json
```

Script 插件其实就是一组MLSQL脚本以及一个plugin.json描述文件。MLSQL脚本之间也通过如下方式引用

```sql
include plugin.`binlog2delta/lib/a.mlsql`;
```

用下面的命令打包成jar包：

```
jar cf binlog2delta-0.1.0-SNAPSHOT.jar binlog2delta/*
```

 
plugin.json文件描述了所有需要设置的参数以及入口类。并且console会自动为其生成向导：

![](https://docs.mlsql.tech/upload_images/WX20190916-183140@2x.png)

具体大家参看[示例项目](https://github.com/allwefantasy/mlsql-pluins/tree/master/binlog2delta)


## 一些帮助命令

查看所有script插件：

```sql
load delta.`__mlsql__.plugins` as plugins;
select * from plugins where pluginType="script" as output;
```

显示script插件里的文件的内容：

```
!plugin script show binlog2delta/plugin.json;
```









## 如何部署你的插件

你需要将插件使用shade模式打成jar包然后提交到应用商店，之后用户可以直接使用!plugin命令安装。