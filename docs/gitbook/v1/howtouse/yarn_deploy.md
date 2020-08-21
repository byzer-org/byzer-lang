# Yarn部署

Yarn部署非常简单。我们推荐使用yarn-client模式部署。根据[Local部署](http://docs.mlsql.tech/mlsql-engine/howtouse/deploy.html)的需求，下载Spark/Engine，然后按如下步骤即可启动。

1. 将hdfs/yarn/hive相关xml配置文件放到 SPARK_HOME/conf目录下。
2. 修改Engine的`start-local.sh`

## 修改Engine的`start-local.sh`

找到文件里如下代码片段：

```shell
$SPARK_HOME/bin/spark-submit --class streaming.core.StreamingApp \
        --driver-memory ${DRIVER_MEMORY} \
        --jars ${JARS} \
        --master local[*] \
        --name mlsql \
        --conf "spark.sql.hive.thriftServer.singleSession=true" \
```

将--master的local[*] 换成 yarn-client, 然后添加excuctor配置,最后大概如下面的样子：

```shell
$SPARK_HOME/bin/spark-submit --class streaming.core.StreamingApp \
        --driver-memory ${DRIVER_MEMORY} \
        --jars ${JARS} \
        --master yarn \
        --deploy-mode client \
        --executor-memory 2g \
        --executor-cores 1 \
        --num-executors 1 \
        --name mlsql \
        --conf "spark.sql.hive.thriftServer.singleSession=true" \
```

然后指定SPARK_HOME运行即可。

