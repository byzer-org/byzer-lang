# StreamingPro 中文文档


1. [概述](https://github.com/allwefantasy/streamingpro/blob/master/README-CN.md#概述)
1. [编译](https://github.com/allwefantasy/streamingpro/blob/master/README-CN.md#编译)
1. [项目模块说明](https://github.com/allwefantasy/streamingpro/blob/master/README-CN.md#项目模块说明)
1. [相关概念](https://github.com/allwefantasy/streamingpro/blob/master/README-CN.md#相关概念)
1. [第一个流式程序](https://github.com/allwefantasy/streamingpro/blob/master/README-CN.md#第一个流式程序)
1. [StreamingPro的一些参数](https://github.com/allwefantasy/streamingpro/blob/master/README-CN.md#StreamingPro的一些参数)
1. [执行一个批处理任务](https://github.com/allwefantasy/streamingpro/blob/master/README-CN.md#执行一个批处理任务)
1. [启动一个SQL server服务](https://github.com/allwefantasy/streamingpro/blob/master/README-CN.md#启动一个SQL-server服务)
1. [基于StreamingPro编程](https://github.com/allwefantasy/streamingpro/blob/master/README-CN.md#基于StreamingPro编程)
1. [对Flink的支持](https://github.com/allwefantasy/streamingpro/blob/master/README-CN.md#对Flink的支持)
1. [StreamingPro Manager](https://github.com/allwefantasy/streamingpro/blob/master/README-CN.md#StreamingPro-Manager)
1. [StreamingPro json文件编辑器支持](https://github.com/allwefantasy/streamingpro/blob/master/README-CN.md#StreamingPro-json文件编辑器支持)
1. [StreamingPro对机器学习的支持](https://github.com/allwefantasy/streamingpro/blob/master/README-CN.md#StreamingPro对机器学习的支持)







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

| 模块名	 | 描述  |备注 |
|:-----------|:------------|:------------|
|streamingpro-commons | 一些基础工具类||
|streamingpro-spark-common | Spark有多个版本，所以可以共享一些基础的东西||
|streamingpro-flink |  streamingpro对flink的支持||
|streamingpro-spark  | streamingpro对spark 1.6.x的支持||
|streamingpro-spark-2.0 | streamingpro对spark 2.x的支持||
|streamingpro-api | streamingpro把底层的spark API暴露出来，方便用户灵活处理问题||
|streamingpro-manager | 通过该模块，可以很方便的通过web界面启动，管理，监控 spark相关的应用||

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


## 执行一个批处理任务

如果我希望把数据库的某个表的数据同步成parquet文件，这个通过StreamingPro是一件极其简单的事情，我们写一个简单的配置文件：

```
{
  "mysql-table-export-to-parquet": {
    "desc": "把mysql表同步成parquet文件",
    "strategy": "spark",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
          "name": "batch.sources",
          "params": [
            {
               url:"jdbc:mysql://localhost/test?user=fred&password=secret",
              "dbtable":"table1",
              "driver":"com.mysql...",
              "path": "-",
              "format": "jdbc",
              "outputTable": "test",

            },
            {
              "path": "/user/data/a.json",
              "format": "json",
              "outputTable": "test2",
              "header": "true"
            }
          ]
       },
      {
        "name": "batch.sql",
        "params": [
          {
            "sql": "select test.* from test left join test2 on test.id=test2.id2",
            "outputTableName": "tempTable1"
          }
        ]
      },
     {
      "name": "batch.sql",
      "params": [
        {
          "sql": "select test.* from tempTable1 left join test2 on tempTable1.id=test2.id2",
          "outputTableName": "tempTable2"
        }
      ]
        },
      {
        "name": "batch.outputs",
        "params": [
          {
            "name":"jack",
            "format": "parquet",
            "path": "/tmp/parquet1",
            "inputTableName": "tempTable2",
            "mode":"Overwrite"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```

这个例子显示了如何配置多个数据源，并且sql可以如何进行交互，最后如何进行输出。batch.sources,batch.sql,batch.outputs完全是
以表来进行连接的，我们可以使用很多sql，通过生成中间表的形式来较为简单的完成一个任务。

batch.sql 目前只能配置一条sql语句，但是一个配置文件可以写很多batch.sql。batch.sql之间可以互相依赖，并且有顺序之分。每个batch.sql
都需要指定一个输出表，除非你执行的ddl语句。

## 执行Structured Streaming任务

原生Structured Streaming不支持Kafka 0.8,0.9,所以StreamingPro提供了对老版本的支持，你可以通过kafka8,kakfa9来即可。如果是kafka 1.0之后，
那么直接用kafka 作为format即可。和Spark Streaming一样，一个Job里只能包含一个Kafka源。一个简单示例如下：



```
{
  "your-fist-ss-job": {
    "desc": "测试",
    "strategy": "spark",
    "algorithm": [],
    "ref": [
    ],
    "compositor": [
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
      },
      {
        "name": "ss.sql",
        "params": [
          {
            "sql": "select city as value from test left join sample on  CAST(test.value AS String) == sample.name",
            "outputTableName": "test3"
          }
        ]
      },
      {
        "name": "ss.outputs",
        "params": [
          {
            "mode": "append",
            "format": "kafka8",
            "metadata.broker.list":"127.0.0.1:9092",
            "topics":"test2",
            "inputTableName": "test3",
            "checkpoint":"/tmp/ss-kafka/",
            "path": "/tmp/ss-kafka-data"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```

这个例子从Kafka读取，经过处理后写入Kafka的另外一个topic

## 启动一个SQL server服务

StreamingPro极大的简化了SQL Server，并且支持使用Rest形式的接口。你指要准备一个只包含

```
{}
```
的query.json的文件，然后就可以启动一个Server。具体指令如下：

```
SHome=/Users/allwefantasy/streamingpro

./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[2] \
--name sql-interactive \
$SHome/streamingpro-spark-2.0-0.4.15-SNAPSHOT.jar    \
-streaming.name sql-interactive    \
-streaming.job.file.path file://$SHome/query.json \
-streaming.platform spark   \
-streaming.rest true   \
-streaming.driver.port 9003   \
-streaming.spark.service true \
-streaming.thrift true \
-streaming.enableHiveSupport true
```

之后你就可以通过http协议进行查询了。

我们先通过接口创建一张表：

```
//CREATE TABLE IF NOT EXISTS zhl_table(id string, name string, city string, age Int)
curl --request POST \
  --url http://127.0.0.1:9003/run/sql \
  --data 'sql=CREATE%20TABLE%20IF%20NOT%20EXISTS%20zhl_table(id%20string%2C%20name%20string%2C%20city%20string%2C%20age%20Int)%20'
```

然后创建一个csv格式的数据，然后按如下方式导入：

```
//LOAD DATA LOCAL INPATH  '/Users/allwefantasy/streamingpro/sample.csv'  INTO TABLE zhl_table
curl --request POST \
  --url http://127.0.0.1:9003/run/sql \
  --data 'sql=LOAD%20DATA%20LOCAL%20INPATH%20%20'\''%2FUsers%2Fallwefantasy%2Fstreamingpro%2Fsample.csv'\''%20%20INTO%20TABLE%20zhl_table'
```

然后你就可以查询了：

```
//sql: SELECT * FROM zhl_table
curl --request POST \
  --url http://127.0.0.1:9003/run/sql \
  --data 'sql=SELECT%20*%20FROM%zhl_table'
```

当然，因为我们开启了thrift server，你也可以写程序链接这个服务：

```
object ScalaJdbcConnectSelect {

  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:hive2://localhost:10000/default"

    // there's probably a better way to do this
    var connection:Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM zhl_table ")
      while ( resultSet.next() ) {
        println(" city = "+ resultSet.getString("city") )
      }
    } catch {
      case e => e.printStackTrace
    }
    connection.close()
  }

}
```

有的时候，spark计算时间非常长，我们希望任务丢给spark计算，然后计算好了，再通知我们，streamingpro也支持这种功能。具体做法
如下：

```
curl --request POST \
  --url http://127.0.0.1:9003/run/sql \
  --data 'sql=select%20*%20from%20zhl_table&async=true&resultType=file&path=%2Ftmp%2Fjack&callback=http%3A%2F%2F127.0.0.1%3A9003%2Fpull'
```

[](http://upload-images.jianshu.io/upload_images/1063603-230d2c4387b8903c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

具体参数如下：

| 参数名称	 | 默认值  |说明 |
|:-----------|:------------|:------------|
|async| false|是否异步执行||
|sql | 查询SQL||
|path| 无|spark的查询结果会先临时写入到这个目录|
|callback| 无|StreamingPro会调用该参数提供的接口告知下载地址。|
|tableName| 无|如果该参数被配置，那么数据会被写入对应的hive表|
|resultType| 无|async=false时，如果该参数被设置并且file时，那么接口返回一个地址而非结果|


## 基于StreamingPro编程

通过添加UDF函数，可以很好的扩充SQL的功能。
具体做法是，首先，在配置文件添加一个配置，

```
"udf_register": {
    "desc": "测试",
    "strategy": "refFunction",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "sql.udf",
        "params": [
          {
            "analysis": "streaming.core.compositor.spark.udf.func.Functions"
          }
        ]
      }
    ]
  }
```

udf_register, analysis等都可以自定义命名，最好是取个有意义的名字，方便管理。

`streaming.core.compositor.spark.udf.func.Functions `包含了你开发的UDF函数。比如我要开发一个mkString udf函数：

```
object Functions {
  def mkString(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("mkString", (sep: String, co: mutable.WrappedArray[String]) => {
      co.mkString(sep)
    })
  }
}
```

之后就可以在你的Job的ref标签上引用了

```
{
  "your-first-batch-job": {
    "desc": "测试",
    "strategy": "spark",
    "algorithm": [],
    "ref": ['udf_register'],
```

`your-first-batch-job` 下所有的batch.sql 就可以使用这个自定义的`mkString` 函数了。

另外，StreamingPro也支持script脚本（目前只支持scala脚本），因为在配置文件中，如果能嵌入一些脚本，在特定场景里也是很方便的，
这样既不需要编译啥的了。截止到这篇发布为止,支持脚本的有：

Spark 1.6.+:

    * 批处理

Spark 2.+:

     * 批处理
     * Spark Streaming处理

具体做法是使用batch.script.df 算子：

```
{
        "name": "batch.script.df",
        "params": [
          {
            "script": "context.sql(\"select a as t from test\").registerTempTable(\"finalOutputTable\")",
            "source": "-"
          }
        ]
      }
```

给出一个比较完整的例子：

```
{
  "batch-console": {
    "desc": "测试",
    "strategy": "spark",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "batch.sources",
        "params": [
          {
            "path": "file:///tmp/hdfsfile/abc.txt",
            "format": "json",
            "outputTable": "test"

          }
        ]
      },
      {
        "name": "batch.script.df",
        "params": [
          {
            "script": "context.sql(\"select a as t from test\").registerTempTable(\"finalOutputTable\")",
            "source": "-"
          }
        ]
      },
      {
        "name": "batch.outputs",
        "params": [
          {
            "name":"jack",
            "format": "console",
            "path": "-",
            "inputTableName": "finalOutputTable",
            "mode":"Overwrite"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```

在json中写代码是一件很复杂的事情，你也可以把代码放在另外一个文件中，然后引用该文件即可，具体做法如下，

```
{
        "name": "batch.script.df",
        "params": [
          {
            "script": "file:///tmp/raw_process.scala",
            "source": "file"
          }
        ]
},
```

前面的案例是暴露了sqlContext给你，显得有点太灵活，而且这个方案因为使用了动态编译，有部分场景会有异常。所以一个更好的办法是
依然使用表作为交互的方式，具体使用如下：


```
{
        "name": "batch.script",
        "params": [
          {
            "inputTableName": "test",
            "outputTableName": "test3",
            "schema": "file:///tmp/raw_schema.scala",
            "useDocMap":true
          },
          {
            "raw": "file:///tmp/raw_process.scala"
          }
        ]
      },
```

其中raw 是一段scala代码。我们定义了inputTableName作为输入，那么这段代码就是处理这个表的，你需要给出输出，以及对应的
输出的schema类型。

/tmp/raw_process.scala 的代码如下：

```
val Array(a,b)=doc("raw").toString.split("\t")
           Map("a"->a,"b"->b)

```

doc其实就是inputTableName,这是一个Map[String,Any]结构的数据。

/tmp/raw_schema.scala" 的代码如下：

```
Some(StructType(Array(StructField("a", StringType, true),StructField("b", StringType, true))))
```


StreamingPro也提供了API,可以定制任何你要的环节，并且和其他现有的组件可以很好的协同，当然，你也可以使用原始的Compositor接口，
实现 非常高级的功能。目前支持的版本和类型有：
Spark 2.+:

     * 批处理
     * Spark Streaming处理


这里有个spark streaming的例子，我想先对数据写代码处理，然后再接SQL组件，然后再进行存储（存储我也可能想写代码）
```
{
  "scalamaptojson": {
    "desc": "测试",
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
        "name": "stream.script.df",
        "params": [
          {
            "clzz": "streaming.core.compositor.spark.api.example.TestTransform",
            "source": "-"
          }
        ]
      },
      {
        "name": "stream.sql",
        "params": [
          {
            "sql": "select * from test2",
            "outputTableName": "test3"
          }
        ]
      },
      {
        "name": "stream.outputs",
        "params": [
          {
            "clzz": "streaming.core.compositor.spark.api.example.TestOutputWriter",
            "inputTableName": "test3"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```

要实现上面的逻辑，首先是创建一个项目，然后引入下面的依赖：

```
  <dependency>
            <groupId>streaming.king</groupId>
            <artifactId>streamingpro-api</artifactId>
            <version>2.0.0</version>
        </dependency>
```
这个包目前很简单，只有两个接口：

```
//数据处理
trait Transform {
  def process(sQLContext: SQLContext, contextParams: Map[Any, Any], config: Map[String, String]): Unit
}

//数据写入
trait OutputWriter {
  def write(df: DataFrame, contextParams: Map[Any, Any], config: Map[String, String]): Unit
}
```

以数据处理为例，只要实现Transform接口，就可以通过stream.script.df 模块进行配置了。
```
 {
        "name": "stream.script.df",
        "params": [
          {
            "clzz": "streaming.core.compositor.spark.api.example.TestTransform",
            "source": "-"
          }
        ]
      },
```

同样，我们也对输出进行了编程处理。
下面是TestTransform的实现：

```
class TestTransform extends Transform {
  override def process(sQLContext: SQLContext, contextParams: Map[Any, Any], config: Map[String, String]): Unit = {
    sQLContext.sql("select * from test").createOrReplaceTempView("test2")
  }
}
```

TestOutputWriter也是类似的：

```
class TestOutputWriter extends OutputWriter {
  override def write(sQLContext: SQLContext, contextParams: Map[Any, Any], config: Map[String, String]): Unit = {
    sQLContext.sparkSession.table(config("inputTableName")).show(100)
  }
}
```

contextParams 是整个链路传递的参数，大家可以忽略。config则是配置参数，比如如上面配置中的source参数，clzz参数等。另外这些参数都是可以通过启动脚本配置和替换的，参看[如何在命令行中指定StreamingPro的写入路径](http://www.jianshu.com/p/edaa0c124933)


## 对Flink的支持

进入flink安装目录运行如下命令：

```
./bin/start-local.sh
```

之后写一个flink.json文件：


```
{
  "flink-example": {
    "desc": "测试",
    "strategy": "flink",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "flink.sources",
        "params": [
          {
            "format": "socket",
            "port": "9000",
            "outputTable": "test"
          }
        ]
      },
      {
        "name": "flink.sql",
        "params": [
          {
            "sql": "select * from test",
            "outputTableName": "finalOutputTable"
          }
        ]
      },
      {
        "name": "flink.outputs",
        "params": [
          {
            "name":"jack",
            "format": "console",
            "inputTableName": "finalOutputTable"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```
目前source 只支持 kafka/socket ，Sink则只支持console和csv。准备好这个文件你就可以提交任务了：

./bin/flink run  -c streaming.core.StreamingApp \ /Users/allwefantasy/streamingpro/streamingpro.flink-0.4.14-SNAPSHOT-online-1.2.0.jar
-streaming.name god \
-streaming.platform flink_streaming \
-streaming.job.file.path file:///Users/allwefantasy/streamingpro/flink.json

然后皆可以了。

你也可以到localhost:8081 页面上提交你的任务。

## StreamingPro Manager



StreamingPro中的 streamingpro-manager 提供了部署，管理Spark任务的Web界面。轻量易用。



编译streamingpro-manager:

```
git clone https://github.com/allwefantasy/streamingpro.git
mvn clean package  -pl streamingpro-manager -am  -Ponline -Pscala-2.11  -Pshade
```

之后你应该在streamingpro-manager/target有个jar包,另外streamingpro-manager 的resource-local 目录有个sql文件，大家可以根据其创建表库。

## 启动以及启动参数

```
java -cp ./streamingpro-manager-0.4.15-SNAPSHOT.jar streaming.App \
-yarnUrl yarn resource url地址 比如master.host:8080 \
-jdbcPath  /tmp/jdbc.properties \
-envPath   /tmp/env.properties
```
jdbcPath指定jdbc连接参数，比如：

```
url=jdbc:mysql://127.0.0.1:3306/spark_jobs?characterEncoding=utf8
userName=wow
password=wow
```

你也可以把这些参数写到启动参数里。但是前面加上jdbc.前缀就可。比如：

```
java -cp ./streamingpro-manager-0.4.15-SNAPSHOT.jar streaming.App \
-yarnUrl yarn resource url地址 比如master.host:8080 \
-jdbc.url a \
-jdbc.userName a \
-jdbc. password b
```
envPath 里面放置的是你为了使用spark-submit 需要配置的一些参数，比如：

```
export SPARK_HOME=/opt/spark-2.1.1;export HADOOP_CONF_DIR=/etc/hadoop/conf;cd $SPARK_HOME;
```

我们简单看下streamingpro-manager的界面。

第一个界面是上传Jar包：

![WX20170716-165826@2x.png](http://upload-images.jianshu.io/upload_images/1063603-451bc340880d5c33.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


第二界面是提交任务：

![WX20170716-165856@2x.png](http://upload-images.jianshu.io/upload_images/1063603-564f03134084fc66.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

勾选依赖的jar包，选择主jar包，然后做一些参数配置，然后点击提交会进入一个进度界面。

第三个界面是管理页面。

![WX20170716-165808@2x.png](http://upload-images.jianshu.io/upload_images/1063603-7694d3cfc367e138.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

任务能够被监控是要求已经在Yarn上申请到了applicationid。所以如果提交失败了，点击监控按钮是无效的。如果你的程序已经提交过一次并且获得过applicationid，那么你点击监控后，程序30s会扫描一次，并且自动拉起那些没有在运行的程序（比如失败了或者自己跑完了）。


## StreamingPro json文件编辑器支持

StreamingPro在内部已经用在比较复杂的项目上了。所以导致配置文件巨复杂，之前同事提到这事，然后我自己把配置代码拉下来，看了下确实如此。
一开始想着能否利用其它格式，比如自定义的，或者换成XML/Yaml等，后面发现JSON其实已经算是不错的了，项目大了，怎么着都复杂。
后面反复思量，大致从编辑器这个方向做下enhance,可能可以简化写配置的人的工作量。所以有了这个项目。

因为是StreamingPro的一个辅助工具，所以也就直接开源出来了。代码还比较粗糙，相信后续会不断完善。[streamingpro-editor2](https://github.com/allwefantasy/streamingpro-editor2) 。

jar包下载：到目录  https://pan.baidu.com/s/1jIjylRw 下找到 streamingpro-editor2.jar 文件。


安装：

打开配置界面，选择plugins,然后点选红框，从disk进行安装：

![WX20170405-115306@2x.png](http://upload-images.jianshu.io/upload_images/1063603-03c89f124406666c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

选择你的jar然后restart idea intellij 即可。

使用示例：

新建一文件，举个例子，叫做batch.streamingpro。 看标志，就可以发现这是一个标准的json格式文件。大家会发现菜单栏多了一个选项：


![WX20170405-120006@2x.png](http://upload-images.jianshu.io/upload_images/1063603-04b71c832a9a89d1.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

其实就是一个模板功能。

在batch.streamingpro里写填写batch,然后点选 expandCode（你也可以去重新设置一个快捷键），


![WX20170405-120228@2x.png](http://upload-images.jianshu.io/upload_images/1063603-24fdb4113d4f0d1a.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

然后就会自动扩展成：


![WX20170405-120243@2x.png](http://upload-images.jianshu.io/upload_images/1063603-fb0f46f9f34a532c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

把 your-name 换成你需要的job名字。 然后我们填写下数据源


![WX20170405-120420@2x.png](http://upload-images.jianshu.io/upload_images/1063603-e9467eaeef7069d3.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

运行expandCode,然后就会自动扩展为：


![WX20170405-120548@2x.png](http://upload-images.jianshu.io/upload_images/1063603-04a7bffef1ed9bbb.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

把鼠标移动到format后的双引号内，点击菜单 Code-> Completition -> Basic (你可以用你的快捷键)，然后就会提示有哪些数据源可以用：


![WX20170405-120607@2x.png](http://upload-images.jianshu.io/upload_images/1063603-47cbc9ef295fe8c4.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

如果你大致知道数据源的名称，那么会自动做提示：


![WX20170405-120822@2x.png](http://upload-images.jianshu.io/upload_images/1063603-0f2e559114f03774.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

JDBC的参数其实很多，你也可以通过Code-> Completition -> Basic 来进行提示：


![WX20170405-120937@2x.png](http://upload-images.jianshu.io/upload_images/1063603-576058ac65bac54c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

接着你可以通过相同的方式添加batch.sql,batch.outputs,batch.script,batch.script.df模块,操作也是大体相同的。

SQL编辑支持：

另外streamingpro-editor2也支持sql的编辑。在SQL处点击右键：


![WX20170405-213846@2x.png](http://upload-images.jianshu.io/upload_images/1063603-853504ed8d3dd0d9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

点击第一个item, "sql editor",然后进入编辑界面：


![WX20170405-213721@2x.png](http://upload-images.jianshu.io/upload_images/1063603-def1e59b4babe187.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

目前支持高亮以及换行，双引号自动escape等功能。

## StreamingPro对机器学习的支持

StreamingPro也对机器学习有一定的支持。训练时，数据的输出不是输出到一个存储器，而是输出成一个模型，其他部分和标准的ETL流程是一样的。
一个典型的的线性回归类算法使用如下：

```
{
  {
    "alg": {
      "desc": "测试",
      "strategy": "spark",
      "algorithm": [],
      "ref": [],
      "compositor": [
        {
          "name": "batch.sources",
          "params": [
            {
              "path": "file:///tmp/sample_linear_regression_data.txt",
              "format": "libsvm",
              "outputTable": "test"
            }
          ]
        },
        {
          "name": "batch.output.alg",
          "params": [
            {
              "algorithm": "lr",
              "path": "file:///tmp/lr-model",
              "inputTableName":"test"
            },
            {
              "maxIter": 1,
              "regParam": 0.3,
              "elasticNetParam": 0.8
            }
          ]
        }
      ],
      "configParams": {
      }
    }
  }
}

```

batch.output.alg 是一个特殊的输出算子。第一个参数包含了算法名称，模型存储路径，输入的数据表。
后面可以配置多组超参数。
















