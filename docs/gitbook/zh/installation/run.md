# 如何运行

## MLSQL-Engine启动

MLSQL-Engine 是一个标准的Spark应用，可以直接使用spark-submit 运行，一个典型示例如下：

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
        -streaming.enableHiveSupport true
```

所有spark参数使用`--`来指定，之后是MLSQL-Engine Jar包，用户自己需要的jar包通过--jars带上。
所有`-streaming`开始的参数都是MLSQL参数，并且只能放于主Jar之后。大家要注意顺序。

下面是一个最简启动脚本：


```shell
$SPARK_HOME/bin/spark-submit --class streaming.core.StreamingApp \       
        --master local[*] \
        --name mlsql \        
        ${MLSQL_HOME}/libs/${MAIN_JAR}    \
        -streaming.name mlsql    \
        -streaming.platform spark   \
        -streaming.rest true   \
        -streaming.driver.port 9003   \
        -streaming.spark.service true         
```

-streaming 相关参数我们下个章节会介绍。

## MLSQL-Cluster / MLSQL-Console

 MLSQL-Cluster / MLSQL-Console 是标准的Java单机程序。他们都需要数据库支持，在resource里都有对应的db.sql文件，请使用他们进行数据库
 初始化。
 
 同时都是用application.yml文件做配置，大家可以从项目里找到示例。
 
 MLSQL-Cluster 启动方式为：
 
 ```
 #!/usr/bin/env bash
 
 java -cp .:${MLSQL_CLUSTER_JAR} tech.mlsql.cluster.ProxyApplication \
 -config ${MLSQL_CLUSTER_CONFIG_FILE}
 ```
 
 MLSQL-Console启动方位为： 

 ```
#!/usr/bin/env bash

java -cp .:${MLSQL_CONSOLE_JAR} tech.mlsql.MLSQLConsole \
-mlsql_cluster_url ${MLSQL_CLUSTER_URL} \
-my_url ${MY_URL} \
-user_home ${USER_HOME} \
-enable_auth_center ${ENABLE_AUTH_CENTER:-false} \
-config ${MLSQL_CONSOLE_CONFIG_FILE}
 ``` 