#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

function recordStartOrStop() {
    currentIp=${BYZER_IP}
    serverPort=`$BYZER_HOME/bin/get-properties.sh streaming.driver.port`
    echo `date '+%Y-%m-%d %H:%M:%S '`"INFO : [Operation: $1] user:`whoami`, start time:$2, ip and port:${currentIp}:${serverPort}" >> ${BYZER_HOME}/logs/security.log
}

function prepareEnv {
    export BYZER_CONFIG_FILE="${BYZER_HOME}/conf/byzer.properties"
    echo "SPARK_HOME is:$SPARK_HOME"
    echo "BYZER_HOME is:${BYZER_HOME}"
    echo "BYZER_CONFIG_FILE is:${BYZER_CONFIG_FILE}"

    mkdir -p ${BYZER_HOME}/logs
}

function checkRestPort() {
    echo "Checking rest port on ${MACHINE_OS}"
    if [[ $MACHINE_OS == "Linux" ]]; then
        used=`netstat -tpln | grep "$port" | awk '{print $7}' | sed "s/\// /g"`
    elif [[ $MACHINE_OS == "Mac" ]]; then
        used=`lsof -nP -iTCP:$port -sTCP:LISTEN | grep $port | awk '{print $2}'`
    fi
    if [ ! -z "$used" ]; then
        echo "<$used> already listen on $port"
        exit 1
    fi
    echo "${port} is available"
}

function checkIfStopUserSameAsStartUser() {
    startUser=`ps -p $1 -o user=`
    currentUser=`whoami`

    if [ ${startUser} != ${currentUser} ]; then
        echo `setColor 33 "Warning: You started Byzer-lang as user [${startUser}], please stop the instance as the same user."`
    fi
}

function clearRedundantProcess {
    if [ -f "${BYZER_HOME}/pid" ]
    then
        pidKeep=0
        pidRedundant=0
        for pid in `cat ${BYZER_HOME}/pid`
        do
            pidActive=`ps -ef | grep $pid | grep ${BYZER_HOME} | wc -l`
            if [ "$pidActive" -eq 1 ]
            then
                if [ "$pidKeep" -eq 0 ]
                then
                    pidKeep=$pid
                else
                    echo "Redundant Byzer-lang process $pid to running process $pidKeep, stop it."
                    bash ${BYZER_HOME}/bin/kill-process-tree.sh $pid
                    ((pidRedundant+=1))
                fi
            fi
        done
        if [ "$pidKeep" -ne 0 ]
        then
            echo $pidKeep > ${BYZER_HOME}/pid
        else
            rm ${BYZER_HOME}/pid
        fi
        if [ "$pidRedundant" -ne 0 ]
        then
            quit "Byzer-lang is redundant, start canceled."
        fi
    fi
}

function prepareProp() {
    MAIN_JAR=$(ls ${BYZER_HOME}/main|grep 'byzer-lang')
    [[ -z $MAIN_JAR ]] && echo "Byzer-lang main jar does not exist in ${BYZER_HOME}/main " && exit 1

    MAIN_JAR_PATH="${BYZER_HOME}/main/${MAIN_JAR}"

    JARS=$(echo ${BYZER_HOME}/libs/*.jar | tr ' ' ',')",$MAIN_JAR_PATH"
    EXT_JARS=$(echo ${BYZER_HOME}/libs/*.jar | tr ' ' ':')":$MAIN_JAR_PATH"

    BYZER_LOG_PATH="file:${BYZER_HOME}/conf/byzer-server-log4j.properties"

    BYZER_PROP=`$BYZER_HOME/bin/get-properties.sh -byzer`
    SPARK_PROP=`$BYZER_HOME/bin/get-properties.sh -spark`
}

function start(){
    clearRedundantProcess

    [[ -z ${BYZER_HOME} ]]  && echo "{BYZER_HOME} is not set, exit" && exit 1
    if [ -f "${BYZER_HOME}/pid" ]; then
        PID=`cat ${BYZER_HOME}/pid`
        if ps -p $PID > /dev/null; then
          quit "Byzer-lang is running, stop it first, PID is $PID"
        fi
    fi

    ${BYZER_HOME}/bin/check-env.sh || exit 1

    START_TIME=$(date "+%Y-%m-%d %H:%M:%S")

    recordStartOrStop "start" "${START_TIME}"

    prepareEnv

    port=`$BYZER_HOME/bin/get-properties.sh streaming.driver.port`
    checkRestPort

    prepareProp

    echo "Starting Byzer-lang..."
    echo "[Spark config]"
    echo "$SPARK_PROP"

    echo "[Byzer config]"
    echo "${BYZER_PROP}"

    echo "[Extra config]"
    echo "${EXT_JARS}"

    cd $BYZER_HOME/
    nohup $SPARK_HOME/bin/spark-submit --class streaming.core.StreamingApp \
        --jars ${JARS} \
        --conf "spark.driver.extraClassPath=${EXT_JARS}" \
        --driver-java-options "-Dlog4j.configuration=${BYZER_LOG_PATH}" \
        $SPARK_PROP \
        $MAIN_JAR_PATH  \
        $BYZER_PROP >> ${BYZER_HOME}/logs/byzer.out & echo $! >> ${BYZER_HOME}/pid

    sleep 3
    clearRedundantProcess

    [ ! -f "${BYZER_HOME}/pid" ] && quit "Byzer-lang start failed, check via log: ${BYZER_HOME}/logs/byzer.log."

    PID=`cat ${BYZER_HOME}/pid`
    CUR_DATE=$(date "+%Y-%m-%d %H:%M:%S")
    echo $CUR_DATE" new Byzer-lang process pid is "$PID >> ${BYZER_HOME}/logs/byzer.log

    echo "Byzer-lang is starting. It may take a while. For status, please visit http://$BYZER_IP:$port."
    echo "You may also check status via: PID:`cat ${BYZER_HOME}/pid`, or Log: ${BYZER_HOME}/logs/byzer.log."
    recordStartOrStop "start success" "${START_TIME}"
}

function stop(){
#    clearCrontab

    STOP_TIME=$(date "+%Y-%m-%d %H:%M:%S")
    if [ -f "${BYZER_HOME}/pid" ]; then
        PID=`cat ${BYZER_HOME}/pid`
        if ps -p $PID > /dev/null; then

           checkIfStopUserSameAsStartUser $PID

           echo `date '+%Y-%m-%d %H:%M:%S '`"Stopping Byzer-lang: $PID"
           kill $PID
           for i in {1..10}; do
              sleep 3
              if ps -p $PID -f | grep byzer > /dev/null; then
                echo "loop $i"
                 if [ "$i" == "10" ]; then
                    echo `date '+%Y-%m-%d %H:%M:%S '`"Killing Byzer-lang: $PID"
                    kill -9 $PID
                 fi
                 continue
              fi
              break
           done
           rm ${BYZER_HOME}/pid

           recordStartOrStop "stop" "${STOP_TIME}"
           return 0
        else
           return 1
        fi

    else
        return 1
    fi
}

# start command
if [ "$1" == "start" ]; then
    echo "Starting Byzer-lang..."
    start
# stop command
elif [ "$1" == "stop" ]; then
    echo `date '+%Y-%m-%d %H:%M:%S '`"Stopping Byzer-lang..."
    stop
    if [[ $? == 0 ]]; then
        exit 0
    else
        quit "Byzer-lang is not running"
    fi
# restart command
elif [ "$1" == "restart" ]; then
    echo "Restarting Byzer-lang..."
    echo "--> Stopping Byzer-lang first if it's running..."
    stop
    if [[ $? != 0 ]]; then
        echo "    Byzer-lang is not running, now start it"
    fi
    echo "--> Re-starting Byzer-lang..."
    start
else
    quit "Usage: 'byzer.sh [-v] start' or 'byzer.sh [-v] stop' or 'byzer.sh [-v] restart'"
fi