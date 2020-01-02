# MLSQL Stack 简介

MLSQL是一门标准的大数据/机器学习语言，MLSQL Engine则是执行该语言的分布式引擎。他们关系好比Java和JVM。

MLSQL Stack 则是一套解决方案，包含：

1. MLSQL Engine  如前所述，他是真实执行MLSQL的一个分布式引擎。
2. MLSQL Cluster 可以管理多个MLSQL Engine,主要功能目前是路由。
3. MLSQL Console 提供了一个Web控制台，可以理解为是编写MLSQL的一个IDE. 具备复杂的权限控制，脚本管理等。
4. MLSQL Store   提供了一些常见的功能模块,这包括脚本以及扩展Jar包,以及数据源依赖解决。

前三者为开源组件。 

利用MLSQL Stack,可以轻易完成实现批处理，流式处理，机器学习，爬虫，API服务等多领域功能。

## 版本说明

下载站点： [http://download.mlsql.tech/1.5.0-SNAPSHOT/](http://download.mlsql.tech/1.5.0-SNAPSHOT/)

版本说明：

``` 
mlsql-engine_2.4-1.5.0-SNAPSHOT.tar.gz
```

以上面的文件名为例子，第一个数字2.4表示该发行版是基于Spark 2.4编译的，这意味着你需要使用Spark 2.4.3来运行它。
第二个版本号是1.5.0-SNAPSHOT，表示MLSQL自身的版本。



