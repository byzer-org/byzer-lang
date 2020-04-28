#!/bin/bash
set -x

for env in SPARK_HOME ; do
  if [ -z "${!env}" ]; then
    echo "$env must be set to run this script"
    exit 1
  fi
done

if [ -z "${MLSQL_HOME}" ]; then
  export MLSQL_HOME="$(cd "`dirname "$0"`"/.; pwd)"
fi

JARS=$(echo ${MLSQL_HOME}/libs/*.jar | tr ' ' ',')
MAIN_JAR=$(ls ${MLSQL_HOME}/libs|grep 'streamingpro-mlsql')
export DRIVER_MEMORY=${DRIVER_MEMORY:-2g}
$SPARK_HOME/bin/spark-submit --class streaming.core.StreamingApp \
        --driver-memory ${DRIVER_MEMORY} \
        --jars ${JARS} \
        --master local[*] \
        --name mlsql \
        --conf "spark.driver.extraJavaOptions"="-DREALTIME_LOG_HOME=/tmp/__mlsql__/logs" \
        --conf "spark.sql.hive.thriftServer.singleSession=true" \
        --conf "spark.kryoserializer.buffer=256k" \
        --conf "spark.kryoserializer.buffer.max=1024m" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        --conf "spark.scheduler.mode=FAIR" \
        ${MLSQL_HOME}/libs/${MAIN_JAR}    \
        -streaming.name mlsql    \
        -streaming.platform spark   \
        -streaming.ps.enable true \
        -streaming.rest true   \
        -streaming.driver.port 9003   \
        -streaming.spark.service true \
        -streaming.thrift false \
        -streaming.datalake.path /tmp/datalake \
        -streaming.enableHiveSupport true
