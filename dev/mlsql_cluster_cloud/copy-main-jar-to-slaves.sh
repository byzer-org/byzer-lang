echo "copy main jar to slave"

MAIN_JAR=$(ls ${MLSQL_JAR_PATH}/libs|grep 'streamingpro-mlsql')

pymlsql copy-from-local --instance-id ${slave_instance_id} --execute-user root \
--source ${MLSQL_JAR_PATH}/libs/${MAIN_JAR} \
--target /home/webuser

pymlsql copy-from-local --instance-id ${slave_instance_id} --execute-user root \
--source /home/webuser/third-party-jars \
--target /home/webuser

