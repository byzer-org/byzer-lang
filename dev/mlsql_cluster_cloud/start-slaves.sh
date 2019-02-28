#!/usr/bin/env bash

echo "Create instance for slave"
start_output=$(pymlsql start --image-id m-bp13ubsorlrxdb9lmv2x --instance-type ${SLAVE_INSTANCE_TYPE} --init-ssh-key false --security-group ${SECURITY_GROUP} --need-public-ip false)
echo ----"${start_output}"-----
slave_instance_id=$(echo "${start_output}"|grep '^instance_id:'|cut -d ':' -f2)
echo "slave instance id is:${slave_instance_id}"
echo "${slave_instance_id}" >> mlsql.slaves

SCRIPT_FILE="/tmp/${slave_instance_id}"
echo "configure spark slave"

cat << EOF > ${SCRIPT_FILE}
#!/usr/bin/env bash
echo "${inter_ip} ${master_hostname}" >> /etc/hosts
EOF

pymlsql exec-shell --instance-id ${slave_instance_id} \
--script-file ${SCRIPT_FILE} \
--execute-user root


echo "start spark slave"

cat << EOF > ${SCRIPT_FILE}
#!/usr/bin/env bash
source activate mlsql-3.5
export SPARK_HOME=/home/webuser/apps/spark-${MLSQL_SPARK_VERSION}

if [[ "${HDFS_TO_OSS_ENABLE}" == "true" ]];then
 cp /home/webuser/third-party-jars/core-site.xml \${SPARK_HOME}/conf/
 rm \${SPARK_HOME}/jars/hadoop-*.jar
 cp /home/webuser/third-party-jars/*.jar \${SPARK_HOME}/jars
fi

cd \${SPARK_HOME}
./sbin/start-slave.sh spark://${inter_ip}:7077
EOF

pymlsql exec-shell --instance-id ${slave_instance_id} \
--script-file ${SCRIPT_FILE} \
--execute-user webuser
