# 如何执行初始化脚本

系统启动时，我们总是希望能够执行一些初始化脚本，比如设置一些共用的临时表，程序重启后
自动注册各个数据源的connect信息。

在MLSQL启动脚本里：

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
        -streaming.rest true   \
        -streaming.driver.port 9003   \
        -streaming.spark.service true \
        -streaming.thrift false \        
        -streaming.enableHiveSupport true \
        -streaming.job.file.path /tmp/init.json
```

我们只关心最后一行，这里可以配置一个json文件。

> -streaming.job.file.path /tmp/init.json 是一个hdfs路径。

在该json文件里，你可以写一个到多个初始化脚本：

```json
{
  "init_job_1": {
    "desc": "系统启动时执行下面的脚本",
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

在这个脚本里，你可以写MLSQL脚本，并且完成一些需要启动时完成的工作，比如执行connect语句。