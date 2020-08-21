# Local部署

Local部署适合开发使用。如果企业服务器单机够强劲，也可以Local部署，性能会好于同等配置的集群。

## 安装条件

1. 下载[Spark发行包](https://spark.apache.org/downloads.html)
2. 下载[MLSQL发行包](http://download.mlsql.tech/2.0.0/)

注意:

* `mlsql-engine_3.0-x.x.x.tar.gz`  对应Spark 3.0.x
* `mlsql-engine_2.4-x.x.x.tar.gz ` 对应spark 2.4.x

## 启动

分别解压两个发行包. 解压后的MLSQL Engine包如下：

![](http://docs.mlsql.tech/upload_images/e6093c4c-bc11-47fc-9b2a-5a08084e5cd6.png)

按如下方式启动`start-default.sh` 即可运行：

```
export SPARK_HOME=... && ./start-default.sh
```

其中SPARK_HOME 是你Spark的解压目录。如果没有设置，脚本会提示你需要设置。
如果一切正常,你应该会看到如下的启动信息：

![](http://docs.mlsql.tech/upload_images/d6139e4f-de5c-4b7f-8d43-5a28e1a25bb7.png)


## 额外配置

可以打开 `start-local.sh`脚本，里面的配置都可以修改：

![](http://docs.mlsql.tech/upload_images/d4942564-b1d4-4771-b6c2-0eab0a9fe134.png)

具体可配置参数参看[启动参数详解](http://docs.mlsql.tech/mlsql-engine/howtouse/configuration.html)


