## 如何运行StreamingPro Server

* 下载资源

你即可以通过自己手动编译一个jar包出来，也可以到[release页面][https://github.com/allwefantasy/streamingpro/releases]下载最新jar包。
一旦获取jar包后，你还需要下载一个[spark发行版](https://www.apache.org/dyn/closer.lua/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz)。

* 解压进入spark home目录，然后运行如下指令即可：

```
./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[2] \
--name sql-interactive \
streamingpro-spark-2.0-1.0.0.jar    \
-streaming.name sql-interactive    \
-streaming.job.file.path file:///tmp/query.json \
-streaming.platform spark   \
-streaming.rest true   \
-streaming.driver.port 9003   \
-streaming.spark.service true \
-streaming.thrift false \
-streaming.enableHiveSupport true
```

query.json是一个包含"{}" 字符串的文件，[更多参数](https://github.com/allwefantasy/streamingpro#streamingpro的一些参数)解释。

大家看到，这是一个标准的Spark程序启动命令。然后以单'-streaming'开头的则是StreamingPro特有的参数。一旦启动后，就可以通过http 9003端口
进行交互了。你可以提交一些SQL脚本即可完成包括爬虫、ETL、流式计算、算法、模型预测等功能。
