echo "copy main jar to slave"

MAIN_JAR=$(ls ${MLSQL_JAR_PATH}/libs|grep 'streamingpro-mlsql')

pymlsql copy-from-local --instance-id ${slave_instance_id} --execute-user root \
--source ${MLSQL_JAR_PATH}/libs/${MAIN_JAR} \
--target /home/webuser

pymlsql copy-from-local --instance-id ${slave_instance_id} --execute-user root \
--source /home/webuser/third-party-jars \
--target /home/webuser

echo "start spark slave"

cat << EOF > ${SCRIPT_FILE}
#!/usr/bin/env bash
source activate mlsql-3.5
export SPARK_HOME=/home/webuser/apps/spark-${MLSQL_SPARK_VERSION}

## replace all jars
if [[ "${HDFS_TO_OSS_ENABLE}" == "true" ]];then
 cp /home/webuser/third-party-jars/core-site.xml \${SPARK_HOME}/conf/
 rm \${SPARK_HOME}/jars/hadoop-*.jar
 cp /home/webuser/third-party-jars/*  \${SPARK_HOME}/jars/
fi


cd \${SPARK_HOME}
./sbin/start-slave.sh spark://${inter_ip}:7077
EOF

pymlsql exec-shell --instance-id ${slave_instance_id} \
--script-file ${SCRIPT_FILE} \
--execute-user webuser

