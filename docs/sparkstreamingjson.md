## 第一个流式程序

 StreamingPro中，compositor的命名都是有迹可循。如果是Spark Streaming,则会以 stream. 开头，如果是Structured Streaming，则会
 以 ss. 开头，普通批处理，则以 batch. 开头。我们这里举的例子会是Spark Streaming.


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

Socket 是个很好的测试途径。如果你需要换成kafka,只需把format换成kafka即可，当然，对应的属性也需要做些调整：

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