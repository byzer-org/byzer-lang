# StreamingPro 中文文档

[五分钟快速上手和体验](https://github.com/allwefantasy/streamingpro/blob/master/docs/quick-tutorial.md)

## 编译

* [编译文档](https://github.com/allwefantasy/streamingpro/blob/master/docs/compile.md)

## 作为服务运行

* [运行StreamingPro Server](https://github.com/allwefantasy/streamingpro/blob/master/docs/run-server.md)

## MLSQL常用功能

* [MLSQL语法说明](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-grammar.md)
* [各数据源读取和写入](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-datasources.md)
* [常见特征处理模型说明](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-data-processing-model.md)
* [常见向量操作Vector操作函数](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-functions.md)
* [如何在MLSQL中运用分词抽词工具](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-analysis.md)

* [网页抓取](https://github.com/allwefantasy/streamingpro/blob/master/docs/crawler.md)
* [批处理计算](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-batch.md)
* [流式计算](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-stream.md)


## 使用MLSQL做机器学习模型训练
* [MLSQL使用SKLearn做文本分类实例](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-nlp-example.md)
* [MLSQL-分布式算法 based on spark MMLib](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-mmlib.md)
* [MLSQL-深度学习 based on TensorFlow](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-tensorflow.md)
* [MLSQL-单机算法 based on SKLearn](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-sklearn.md)
* [MLSQL日志回显](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-log-monitor.md)


## 部署MLSQL模型API
* [模型部署](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-model-deploy.md)

## StreamingPro Server 使用Carbondata做存储
* [如何使用CarbonData作为存储](https://github.com/allwefantasy/streamingpro/blob/master/docs/carbondata.md)


## 使用配置完成Spark编程
1. [Spark 批处理](https://github.com/allwefantasy/streamingpro/blob/master/docs/batchjson.md)
1. [Spark Streaming](https://github.com/allwefantasy/streamingpro/blob/master/docs/sparkstreamingjson.md)
1. [Structured Streaming](https://github.com/allwefantasy/streamingpro/blob/master/docs/structuredstreamingjson.md)
1. [StreamingPro对机器学习的支持](https://github.com/allwefantasy/streamingpro/blob/master/docs/mljson.md)

## 概览：

1. [概述](https://github.com/allwefantasy/streamingpro/blob/master/README.md#概述)
1. [项目模块说明](https://github.com/allwefantasy/streamingpro/blob/master/README.md#项目模块说明)
1. [编译](https://github.com/allwefantasy/streamingpro/blob/master/docs/compile.md)
1. [相关概念](https://github.com/allwefantasy/streamingpro/blob/master/README.md#相关概念)
1. [StreamingPro的一些参数](https://github.com/allwefantasy/streamingpro/blob/master/README.md#StreamingPro的一些参数)

## 周边工具
1. [StreamingPro Manager](https://github.com/allwefantasy/streamingpro/blob/master/docs/manager.md)
1. [StreamingPro json文件编辑器支持](https://github.com/allwefantasy/streamingpro/blob/master/README.md#StreamingPro-json文件编辑器支持)

## 实验
1. [fink支持](https://github.com/allwefantasy/streamingpro/blob/master/docs/flink.md)

## 其他文档

* [英文文档](https://github.com/allwefantasy/streamingpro/blob/master/README-EN.md)
* [简书专栏](https://www.jianshu.com/c/759bc22b9e15)
* [源码阅读-深入浅出StreamingPro](https://github.com/allwefantasy/streamingpro/issues/47)
* [为什么开发MLSQL](https://github.com/allwefantasy/streamingpro/blob/master/docs/why-develop-mlsql.md)
* [SQL服务](https://github.com/allwefantasy/streamingpro/blob/master/docs/sqlservice.md)


## 概述

StreamingPro 支持以Spark,Flink等作为底层分布式计算引擎，通过一套统一的配置文件完成批处理，流式计算，Rest服务的开发。
特点有：

1. 使用Json描述文件完成流式，批处理的开发，不用写代码。
2. 支持SQL Server,支持XSQL/MLSQL（重点），完成批处理，机器学习，即席查询等功能。
3. 标准化输入输出，支持UDF函数注册，支持自定义模块开发
4. 支持Web化管理Spark应用的启动，监控

如果更细节好处有：

1. 跨版本：StreamingPro可以让你不用任何变更就可以轻易的运行在spark 1.6/2.1/2.2上。 
2. 新语法：提供了新的DSl查询语法/Json配置语法
3. 程序的管理工具：提供web界面启动/监控 Spark 程序
4. 功能增强：2.1之后Structured Streaming 不支持kafka 0.8/0.9 ,Structured，此外还有比如spark streaming 支持offset 保存等
5. 简化Spark SQL Server搭建成本：提供rest接口/thrift 接口，支持spark sql server 的负载均衡，自动将driver 注册到zookeeper上
6. 探索更多的吧


## 项目模块说明

| 模块名	 | 描述  |备注 |
|:-----------|:------------|:------------|
|streamingpro-commons | 一些基础工具类||
|streamingpro-spark-common | Spark有多个版本，所以可以共享一些基础的东西||
|streamingpro-flink |  streamingpro对flink的支持||
|streamingpro-spark  | streamingpro对spark 1.6.x的支持||
|streamingpro-spark-2.0 | streamingpro对spark 2.x的支持||
|streamingpro-api | streamingpro把底层的spark API暴露出来，方便用户灵活处理问题||
|streamingpro-manager | 通过该模块，可以很方便的通过web界面启动，管理，监控 spark相关的应用||
|streamingpro-dls | 自定义connect,load,select,save,train,register等语法，便于用类似sql的方式做批处理任务,机器学习等||

## 相关概念

如果你使用StreamingPro，那么所有的工作都是在编辑一个Json配置文件。通常一个处理流程，会包含三个概念：

1. 多个输入
2. 多个连续/并行的数据处理
3. 多个输出

StreamingPro会通过'compositor'的概念来描述他们，你可以理解为一个处理单元。一个典型的输入compositor如下：

```
{
        "name": "batch.sources",
        "params": [
          {
            "path": "file:///tmp/hdfsfile/abc.txt",
            "format": "json",
            "outputTable": "test"

          },
           {
              "path": "file:///tmp/parquet/",
              "format": "parquet",
              "outputTable": "test2"

            }
        ]
}
```

`batch.sources` 就是一个compositor的名字。 这个compositor 把一个本地磁盘的文件映射成了一张表，并且告知系统，abc.txt里的内容
 是json格式的。这样，我们在后续的compositor模块就可以使用这个`test`表名了。通常，StreamingPro希望整个处理流程，
 也就是不同的compositor都采用表来进行衔接。

 StreamingPro不仅仅能做批处理，还能做流式，流式支持Spark Streaming,Structured Streaming。依然以输入compositor为例，假设
 我们使用的是Structured Streaming,则可以如下配置。

 ```
 {
         "name": "ss.sources",
         "params": [
           {
             "format": "kafka9",
             "outputTable": "test",
             "kafka.bootstrap.servers": "127.0.0.1:9092",
             "topics": "test",
             "path": "-"
           },
           {
             "format": "com.databricks.spark.csv",
             "outputTable": "sample",
             "header": "true",
             "path": "/Users/allwefantasy/streamingpro/sample.csv"
           }
         ]
       }
 ```

 第一个表示我们对接的数据源是kafka 0.9，我们把Kafka的数据映射成表test。 因为我们可能还需要一些元数据，比如ip和城市的映射关系，
 所以我们还可以配置一些其他的非流式的数据源，我们这里配置了一个smaple.csv文件，并且命名为表sample。
 
 如果你使用的是kafka >= 1.0,则 topics 参数需要换成'subscribe',并且使用时可能需要对内容做下转换，类似：
  
  ```
  select CAST(key AS STRING) as k, CAST(value AS STRING) as v from test
  ```
  
 启动时，你需要把-streaming.platform 设置为 `ss`。 

 如果我们的输入输出都是Hive的话，可能就不需要batch.sources/batch.outputs 等组件了，通常一个batch.sql就够了。比如：

 ```
 "without-sources-job": {
     "desc": "-",
     "strategy": "spark",
     "algorithm": [],
     "ref": [],
     "compositor": [
       {
         "name": "batch.sql",
         "params": [
           {
             "sql": "select * from hiveTable",
             "outputTableName": "puarquetTable"
           }
         ]
       },
       {
         "name": "batch.outputs",
         "params": [
           {
             "format": "parquet",
             "inputTableName": "puarquetTable",
             "path": "/tmp/wow",
             "mode": "Overwrite"
           }
         ]
       }
     ],
     "configParams": {
     }
   }

 ```

在批处理里，batch.sources/batch.outputs 都是可有可无的，但是对于流式程序，stream.sources/stream.outputs/ss.sources/ss.outputs 则是必须的。


## StreamingPro的一些参数


| Property Name	 | Default  |Meaning |
|:-----------|:------------|:------------|
| streaming.name | (none) required | 等价于 spark.app.name |
| streaming.master | (none) required | 等价于  spark.master |
| streaming.duration | 10 seconds| spark streaming 周期，默认单位为秒 |
| streaming.rest |true/false,default is false | 是否提供http接口|
| streaming.spark.service |true/false,default is false | 开启该选项时，streaming.platform必须为spark. 该选项会保证spark实例不会退出|
| streaming.platform | spark/spark_streaming/ss/flink,default is spark | 基于什么平台跑|
| streaming.checkpoint | (none)|spark streaming checkpoint 目录  |
| streaming.kafka.offsetPath | (none)| kafka的偏移量保存目录。如果没有设置，会保存在内存中 |
| streaming.driver.port | 9003| 配置streaming.rest使用，streaming.rest为true,你可以设置一个http端口   |
| streaming.spark.hadoop.* |(none)| hadoop configuration,eg. -streaming.spark.hadoop.fs.defaultFS hdfs://name:8020  |
| streaming.job.file.path |(none)| 配置文件路径，默认从hdfs加载  |
| streaming.jobs |(none)| json配置文件里的job名称，按逗号分隔。如果没有配置该参数，默认运行所有job  |
| streaming.zk.servers |(none)| 如果把spark作为一个server,那么streamingpro会把driver地址注册到zookeeper上|
| streaming.zk.conf_root_dir |(none)| 配置streaming.zk.servers使用 |
|streaming.enableHiveSupport|false|是否支持Hive|
|streaming.thrift|false|是否thrift server|
| streaming.sql.source.[name].[参数] |(none)| batch/ss/stream.sources 中，你可以替换里面的任何一个参数 |
| streaming.sql.out.[name].[参数] |(none)| batch/ss/stream.outputs 中，你可以替换里面的任何一个参数 |
| streaming.sql.params.[param-name] |(none)| batch/ss/stream.sql中，你是可以写表达式的,比如 select * from :table, 之后你可以通过命令行传递该table参数 |

后面三个参数值得进一步说明：

假设我们定义了两个数据源，firstSource,secondSource,描述如下：

```
{
        "name": "batch.sources",
        "params": [
          {
            "name":"firstSource",
            "path": "file:///tmp/sample_article.txt",
            "format": "com.databricks.spark.csv",
            "outputTable": "article",
            "header":true
          },
          {
              "name":"secondSource",
              "path": "file:///tmp/sample_article2.txt",
              "format": "com.databricks.spark.csv",
              "outputTable": "article2",
              "header":true
            }
        ]
      }
```

我们希望path不是固定的，而是启动时候决定的，这个时候，我们可以在启动脚本中使用-streaming.sql.source.[name].[参数] 来完成这个需求。
比如：

```
-streaming.sql.source.firstSource.path  file:///tmp/wow.txt
```

这个时候，streamingpro启动的时候会动态将path 替换成你要的。包括outputTable等都是可以替换的。

有时候我们需要定时执行一个任务，而sql语句也是动态变化的，具体如下：

```
{
        "name": "batch.sql",
        "params": [
          {
            "sql": "select * from test where hp_time=:today",
            "outputTableName": "finalOutputTable"
          }
        ]
      },
```

这个时候我们在启动streamingpro的时候，通过参数：

```
-streaming.sql.params.today  "2017"
```

动态替换 sql语句里的:today

















