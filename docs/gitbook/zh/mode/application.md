# Application模式

Application实际上是标准的Spark批处理模式，运行完后Spark进程退出。

具体做法如下：


```shell
$SPARK_HOME/bin/spark-submit --class streaming.core.StreamingApp \
        --jars ${JARS} \
        --master local[*] \
        --name mlsql \
        --conf "spark.sql.hive.thriftServer.singleSession=true" \
        --conf "spark.kryoserializer.buffer=256k" \
        --conf "spark.kryoserializer.buffer.max=1024m" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        --conf "spark.scheduler.mode=FAIR" \
        ${MLSQL_HOME}/libs/${MAIN_JAR}    \
        -streaming.name mlsql    \
        -streaming.platform spark   \
        -streaming.rest false   \       
        -streaming.spark.service false \
        -streaming.thrift false \        
        -streaming.enableHiveSupport true \
        -streaming.job.file.path /tmp/job.json
```

你需要在启动时，把`-streaming.rest`，`-streaming.spark.service` ，`-streaming.thrift` 
都设置为false,并且通过`-streaming.job.file.path`配置一个脚本文件。

该脚本文件如下：

```json
{
  "job-1": {
    "desc": "测试",
    "strategy": "spark",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "batch.mlsql",
        "params": [
          {
            "sql": [
              "select 'a' as a as table1;",
              "save overwrite table1 as parquet.`/tmp/kk`;"
            ]
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```

你尅配置多个任务，上面的例子只有一个job-1.如果你希望前面的任务失败，后面的任务就不执行直接退出，可以通过
设置启动参数：

```
-streaming.mode.application.fails_all true
```

默认为false.

这样，你就可以用MLSQL语法+配置文件的方式运行一个Spark批处理程序了。

> 该模式下，即使任务失败，Yarn的状态码依然会是Finish.


