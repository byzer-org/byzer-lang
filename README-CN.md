# StreamingPro 中文文档

## 概述

StreamingPro 支持以Spark,Flink等作为底层分布式计算引擎，通过一套统一的配置文件完成批处理，流式计算，Rest服务的开发。
特点有：

1. 使用Json描述文件实现完全配置化
2. 标准化输入输出，中间处理可以采用SQL，支持UDF函数注册，支持自定义模块开发
3. 支持Web化管理Spark应用的启动，监控

## 编译

步骤一： 下载编译ServiceFramework项目

```
git clone https://github.com/allwefantasy/ServiceFramework.git
cd ServiceFramework
mvn install -Pscala-2.11 -Pjetty-9 -Pweb-include-jetty-9
```
默认是基于Scala 2.11的。如果你想切换到 scala 2.10则使用如下命令：

```
git clone https://github.com/allwefantasy/ServiceFramework.git
cd ServiceFramework
./dev/change-version-to-2.10.sh
mvn install -Pscala-2.10 -Pjetty-9 -Pweb-include-jetty-9
```

步骤2： 下载编译StreamingPro项目

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package  -pl streamingpro-spark-2.0 -am  -Ponline -Pscala-2.11  -Phive-thrift-server -Pspark-2.1.0 -Pshade

```

如果你基于spark 2.2.0 ，那么可以执行如下命令

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package  -pl streamingpro-spark-2.0 -am  -Ponline -Pscala-2.11  -Phive-thrift-server -Pspark-2.2.0 -Pshade

```

默认是编译支持的都是Spark 2.x版本的。如果你希望支持Spark 1.6.x 版本，那么可以采用如下指令：

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package  -pl streamingpro-spark -am  -Ponline -Pscala-2.10  -Pcarbondata -Phive-thrift-server -Pspark-1.6.1 -Pshade
```

## 项目模块说明

streamingpro-commons 一些基础工具类
streamingpro-spark-common  Spark有多个版本，所以可以共享一些基础的东西
streamingpro-flink   streamingpro对flink的支持
streamingpro-spark   streamingpro对spark 1.6.x的支持
streamingpro-spark-2.0 streamingpro对spark 2.x的支持
streamingpro-api  streamingpro把底层的spark API暴露出来，方便用户灵活处理问题
streamingpro-manager  通过该模块，可以很方便的通过web界面启动，管理，监控 spark相关的应用

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


 ## 第一个流式程序

 StreamingPro中，compositor的命名都是有迹可循。如果是Spark Streaming,则会以 stream. 开头，如果是Structured Streaming，则会
 以 ss. 开头，普通批处理，则以 batch. 开哦图。我们这里举得例子会是Spark Streaming.


 ```
 {
   "you-first-streaming-job": {
     "desc": "just a example",
     "strategy": "spark",
     "algorithm": [],
     "ref": [
     ],
     "compositor": [
       {
         "name": "stream.sources",
         "params": [
           {
             "format": "socket",
             "outputTable": "test",
             "port": "9999",
             "host": "localhost",
             "path": "-"
           }
         ]
       },
       {
         "name": "stream.sql",
         "params": [
           {
             "sql": "select avg(value) avgAge from test",
             "outputTableName": "test3"
           }
         ]
       },
       {
         "name": "stream.sql",
         "params": [
           {
             "sql": "select count(value) as nameCount from test",
             "outputTableName": "test1"
           }
         ]
       },
       {
         "name": "stream.sql",
         "params": [
           {
             "sql": "select sum(value) ageSum from test",
             "outputTableName": "test2"
           }
         ]
       },
       {
         "name": "stream.sql",
         "params": [
           {
             "sql": "select * from test1 union select * from test2 union select * from test3",
             "outputTableName": "test4"
           }
         ]
       },
       {
         "name": "stream.outputs",
         "params": [
           {
             "name": "jack",
             "format": "console",
             "path": "-",
             "inputTableName": "test4",
             "mode": "Overwrite"
           }
         ]
       }
     ],
     "configParams": {
     }
   }
 }
 ```

你可以把"you-first-streaming-job" 替换成你想要的名字。一个job是一个处理流程，从输入到输出。你也可以在一个json配置文件里写多个
job,比如：

```
 {
   "you-first-streaming-job": {
     "desc": "just a example",
     "strategy": "spark",
     "algorithm": [],
     "ref": [
     ],
     "compositor": [
     ],
     "configParams": {
     }
   },

   "you-second-streaming-job": {
        "desc": "just a example",
        "strategy": "spark",
        "algorithm": [],
        "ref": [
        ],
        "compositor": [
        ],
        "configParams": {
        }
      }
 }

```

我们最好保持他们之间不存在依赖性。

Socket 是个很好的测试途径。如果你需要换成kafka,只需把format换成kafka即可：

```
{
             "format": "kafka",
             "outputTable": "test",
             "kafka.bootstrap.servers": "127.0.0.1:9092",
             "topics": "test",
             "path": "-"
}
```

值得注意的是，目前对于数据源，StreamingPro一个Job只支持注册一个消息队列,不过好消息是，可以注册多个批处理的数据集，比如一些元数据。
如果你需要使用多个Kafka队列，并且队列之间的数据互相交互，目前来说可能还是有困难的。

你按上面的要求，写好配置文件之后，接着就可以运行这个配置文件：

```
SHome=/Users/allwefantasy/streamingpro

./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[2] \
--name test \
$SHome/streamingpro-spark-2.0-0.4.15-SNAPSHOT.jar    \
-streaming.name test    \
-streaming.platform spark_streaming \
-streaming.job.file.path file://$SHome/spark-streaming.json
```

SHome指向你streamingpro-spark-2.0-0.4.15-SNAPSHOT.jar所在目录。streaming.core.StreamingApp 则是一个固定的写法，所有的
StreamingPro程序都是用这个类作为入口。`-streaming.` 都是streamingpro特有的参数。这里有两个核心参数：

* streaming.platform  目前支持 spark/spark_streaming/ss/flink 四种
* streaming.job.file.path 配置文件的路径。如果是放在jar包里，可以使用 classpath://前缀


这个时候，一个标准的spark streaming程序就运行起来了。

## StreamingPro的一些参数

## Properties


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


## 执行一个批处理任务

如果我希望把数据库的某个表的数据同步成parquet文件，这个通过StreamingPro是一件极其简单的事情。（待续....）






