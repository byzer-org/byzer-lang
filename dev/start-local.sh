#!/bin/bash
#set -x

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

echo
echo "#############"
echo "Run with spark : $SPARK_HOME"
echo "With DRIVER_MEMORY=${DRIVER_MEMORY:-2g}"
echo "Try mannualy to copy https://github.com/allwefantasy/mlsql/blob/master/streamingpro-mlsql/src/main/resources-online/log4j.properties to your SPARK_HOME/CONF"
echo
echo "JARS: ${JARS}"
echo "MAIN_JAR: ${MLSQL_HOME}/libs/${MAIN_JAR}"
echo "#############"
echo
echo
echo
sleep 5

$SPARK_HOME/bin/spark-submit --class streaming.core.StreamingApp \
        --driver-memory ${DRIVER_MEMORY} \
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