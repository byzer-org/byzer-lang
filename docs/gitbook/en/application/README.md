# How to execute initialization script

When the system starts, we want to be able to execute some initialization scripts. For example, 
1. setting up a shared temporary table
2. automatically reregistering the connect of each data source after the system is restarted.

Use the startup script in the MLSQL system below

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

Note the JSON file configured on the last line,

> -streaming.job.file.path /tmp/init.json.  It's a HDFS path.
                                          

One or more initialization scripts can be written in the JSON file
```json
{
  "init_job_1": {
    "desc": "The following script is executed at system startup",
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

In short, you can write MLSQL scripts to complete the work at startup time, such as executing connect statements.

