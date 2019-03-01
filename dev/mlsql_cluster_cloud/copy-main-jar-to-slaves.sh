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
mkdir -p /home/webuser/spark-jars/
cp -r \${SPARK_HOME}/jars /home/webuser/spark-jars/
rm /home/webuser/spark-jars/hadoop-*.jar
cd \${SPARK_HOME}
./sbin/start-slave.sh spark://${inter_ip}:7077
EOF

pymlsql exec-shell --instance-id ${slave_instance_id} \
--script-file ${SCRIPT_FILE} \
--execute-user webuser

