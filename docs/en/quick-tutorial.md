## Quick Tutorial

Step 1:
 
Go to release page: [Release页面](https://github.com/allwefantasy/streamingpro/releases). 
Download jars:

1. streamingpro-mlsql-1.1.1.jar
2. ansj_seg-5.1.6.jar
3. nlp-lang-1.7.8.jar

Step 2:

Go to Spark download page: [Spark](https://spark.apache.org/downloads.html). Choose version 2.2.0.
Unarvhive it.
 
Step 3:

```shell
cd spark-2.2.0-bin-hadoop2.7/

./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[*] \
--name sql-interactive \
--jars ansj_seg-5.1.6.jar,nlp-lang-1.7.8.jar
streamingpro-mlsql-1.1.1.jar    \
-streaming.name sql-interactive    \
-streaming.job.file.path file:///tmp/query.json \
-streaming.platform spark   \
-streaming.rest true   \
-streaming.driver.port 9003   \
-streaming.spark.service true \
-streaming.thrift false \
-streaming.enableHiveSupport true
```

`query.json` is a json file contains "{}".

Step 4: 

Open your chrome browser, type  url following:

```
http://127.0.0.1:9003
```

![](https://github.com/allwefantasy/mlsql-web/raw/master/images/WX20180629-105204@2x.png)

Enjoy.

