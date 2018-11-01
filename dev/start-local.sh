#!/bin/bash
if [ -z "${MLSQL_HOME}" ]; then
  export MLSQL_HOME="$(cd "`dirname "$0"`"/.; pwd)"
fi
JARS=$(echo ${MLSQL_HOME}/libs/*.jar | tr ' ' ',')
$SPARK_HOME/bin/spark-submit --class streaming.core.StreamingApp \
        --jars ${JARS} \
        --master local[*] \
        --name mlsql \
        --conf "spark.sql.hive.thriftServer.singleSession=true" \
        --conf "spark.kryoserializer.buffer=256k" \
        --conf "spark.kryoserializer.buffer.max=1024m" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        --conf "spark.scheduler.mode=FAIR" \
        ${MLSQL_HOME}/libs/streamingpro-mlsql-1.1.3.jar    \
        -streaming.name mlsql    \
        -streaming.job.file.path ${MLSQL_HOME}/empty.json \
        -streaming.platform spark   \
        -streaming.rest true   \
        -streaming.driver.port 9003   \
        -streaming.spark.service true \
        -streaming.thrift false \
        -streaming.enableHiveSupport true
