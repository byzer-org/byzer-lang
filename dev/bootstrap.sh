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
source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

function recordStartOrStop() {
    currentIp=${BYZER_IP}
    serverPort=$($BYZER_HOME/bin/get-properties.sh streaming.driver.port)
    echo $(date '+%Y-%m-%d %H:%M:%S ')"INFO : [Operation: $1] user:$(whoami), start time:$2, ip and port:${currentIp}:${serverPort}" >> ${BYZER_HOME}/logs/security.log
}

function prepareEnv {
    export BYZER_CONFIG_FILE="${BYZER_HOME}/conf/byzer.properties"
    echo "SPARK_HOME is: $SPARK_HOME"
    echo "BYZER_HOME is: ${BYZER_HOME}"
    echo "BYZER_CONFIG_FILE is: ${BYZER_CONFIG_FILE}"

    mkdir -p ${BYZER_HOME}/logs
}

function checkIfStopUserSameAsStartUser() {
    startUser=$(ps -p $1 -o user=)
    currentUser=$(whoami)

    if [ ${startUser} != ${currentUser} ]; then
        echo $(setColor 33 "Warning: You started Byzer-lang as user [${startUser}], please stop the instance as the same user.")
    fi
}

function clearRedundantProcess {
    if [ -f "${BYZER_HOME}/pid" ]
    then
        pidKeep=0
        pidRedundant=0
        for pid in $(cat ${BYZER_HOME}/pid)
        do
            pidActive=$(ps -ef | grep $pid | grep ${BYZER_HOME} | wc -l)
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
    FULL_JARS=$(echo ${BYZER_HOME}/plugin/*.jar | tr ' ' ',')",$JARS"

    EXT_JARS=$(echo ${BYZER_HOME}/libs/*.jar | tr ' ' ':')":$MAIN_JAR_PATH"
    ## Put 3rd-third plugin jars in classpath
    EXT_JARS=$(echo ${BYZER_HOME}/plugin/*.jar | tr ' ' ':')":$EXT_JARS"

    BYZER_LOG_PATH="file:${BYZER_HOME}/conf/byzer-server-log4j2.properties"

    BYZER_PROP=$($BYZER_HOME/bin/get-properties.sh -byzer)
    SPARK_PROP=$($BYZER_HOME/bin/get-properties.sh -spark)
    ALL_PROP=$($BYZER_HOME/bin/get-properties.sh -args)
}

function start(){
    clearRedundantProcess

    # check $BYZER_HOME
    [[ -z ${BYZER_HOME} ]] && quit "{BYZER_HOME} is not set, exit"
    if [ -f "${BYZER_HOME}/pid" ]; then
        PID=$(cat ${BYZER_HOME}/pid)
        if ps -p $PID > /dev/null; then
          quit "Byzer-lang is running, stop it first, PID is $PID"
        fi
    fi

    # check $SPARK_HOME
    if [ $BYZER_SERVER_MODE == "server" ]; then
        # only in server mode need check spark home
        [[ -z ${SPARK_HOME} ]]  && quit "{SPARK_HOME} is not set, exit"
    fi

    ${BYZER_HOME}/bin/check-env.sh || exit 1

    START_TIME=$(date "+%Y-%m-%d %H:%M:%S")

    recordStartOrStop "start" "${START_TIME}"

    prepareEnv

    prepareProp

    echo "Starting Byzer engine in ${BYZER_SERVER_MODE} mode..."

    cd $BYZER_HOME/

        echo ""
        echo "[Java Env]"
        echo "JAVA_HOME: ${JAVA_HOME}"
        echo "JAVA: ${JAVA}"

    DRY_RUN=$($BYZER_HOME/bin/get-properties.sh byzer.server.dryrun)



    if [[ $BYZER_SERVER_MODE = "all-in-one" ]]; then
        echo ""
        echo "[All Config]"
        echo "${ALL_PROP}"
        echo ""

        BYZER_SERVER_MEMORY=$($BYZER_HOME/bin/get-properties.sh byzer.server.runtime.driver-memory)
        
        if [[ "${BYZER_SERVER_MEMORY}" != "" ]];then
           BYZER_SERVER_MEMORY="-Xmx${BYZER_SERVER_MEMORY}"
        else
           BYZER_SERVER_MEMORY="-Xmx4g"
        fi

        echo "Final command:"
        echo "nohup $JAVA ${BYZER_SERVER_MEMORY}"
        echo "-cp ${BYZER_HOME}/main/${MAIN_JAR}:${BYZER_HOME}/spark/*:${BYZER_HOME}/libs/*:${BYZER_HOME}/plugin/*"
        echo "tech.mlsql.example.app.LocalSparkServiceApp $ALL_PROP >> ${BYZER_HOME}/logs/byzer.out &"
        
        if [[ "${DRY_RUN}" != "true" ]];then
          nohup $JAVA ${java_options} \
                      -cp ${BYZER_HOME}/main/${MAIN_JAR}:${BYZER_HOME}/spark/*:${BYZER_HOME}/libs/*:${BYZER_HOME}/plugin/* \
                      tech.mlsql.example.app.LocalSparkServiceApp \
                      $ALL_PROP >> ${BYZER_HOME}/logs/byzer.out &
          echo $! >> ${BYZER_HOME}/pid
        fi


    elif [[ $BYZER_SERVER_MODE = "server" ]]; then
        echo ""
        echo "[Spark Config]"
        echo "$SPARK_PROP"

        echo "[Byzer Config]"
        echo "${BYZER_PROP}"

        echo "[Extra Config]"
        echo "${EXT_JARS}"
        echo ""

        driver_java_options=$($BYZER_HOME/bin/get-properties.sh spark.driver.extraJavaOptions)

        BYZER_RUNTIME_PARAMS=$($BYZER_HOME/bin/get-properties.sh _ -prefix byzer.server.runtime. -type runtime)
        BYZER_RUNTIME_PARAMS=$(echo "$BYZER_RUNTIME_PARAMS" | tr '\n' ' ')
        
        echo "Final command:"
        echo "nohup $SPARK_HOME/bin/spark-submit --class streaming.core.StreamingApp "
        echo "$BYZER_RUNTIME_PARAMS  --jars ${FULL_JARS}"
        echo "--conf spark.driver.extraClassPath=${EXT_JARS}"
        echo "--conf \"spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=${BYZER_LOG_PATH}\" ${driver_java_options}"
        echo "$SPARK_PROP"
        echo "$MAIN_JAR_PATH"
        echo "$BYZER_PROP >> ${BYZER_HOME}/logs/byzer.out &"

        if [[ "${DRY_RUN}" != "true" ]];then
          nohup $SPARK_HOME/bin/spark-submit $BYZER_RUNTIME_PARAMS --class streaming.core.StreamingApp \
                      --jars ${FULL_JARS} \
                      --conf "spark.driver.extraClassPath=${EXT_JARS}" \
                      --conf "spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=${BYZER_LOG_PATH} ${driver_java_options}" \
                      $SPARK_PROP \
                      $MAIN_JAR_PATH  \
                      $BYZER_PROP >> ${BYZER_HOME}/logs/byzer.out &
          echo $! >> ${BYZER_HOME}/pid
        fi
    fi

    sleep 3
    clearRedundantProcess

    [ ! -f "${BYZER_HOME}/pid" ] && quit "Byzer engine start failed, check via log: ${BYZER_HOME}/logs/byzer-lang.log."

    PID=$(cat ${BYZER_HOME}/pid)
    CUR_DATE=$(date "+%Y-%m-%d %H:%M:%S")
    echo $CUR_DATE" new Byzer engine process pid is "$PID >> ${BYZER_HOME}/logs/byzer-lang.log

    echo ""
    echo $(setColor 33 "Byzer engine is starting. It may take a while. For status, please visit http://$BYZER_IP:$BYZER_LANG_PORT.")
    echo ""
    echo "You may also check status via: PID:$(cat ${BYZER_HOME}/pid), or Log: ${BYZER_HOME}/logs/byzer-lang.log."
    recordStartOrStop "start success" "${START_TIME}"
}

function stop(){
#    clearCrontab

    STOP_TIME=$(date "+%Y-%m-%d %H:%M:%S")
    if [ -f "${BYZER_HOME}/pid" ]; then
        PID=$(cat ${BYZER_HOME}/pid)
        if ps -p $PID > /dev/null; then

           checkIfStopUserSameAsStartUser $PID

           echo $(date '+%Y-%m-%d %H:%M:%S ')"Stopping Byzer-lang: $PID"
           kill $PID
           for i in {1..10}; do
              sleep 3
              if ps -p $PID -f | grep byzer > /dev/null; then
                echo "loop $i"
                 if [ "$i" == "10" ]; then
                    echo $(date '+%Y-%m-%d %H:%M:%S ')"Killing Byzer-lang: $PID"
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
    echo "Starting Byzer engine..."
    start
# stop command
elif [ "$1" == "stop" ]; then
    echo $(date '+%Y-%m-%d %H:%M:%S ')"Stopping Byzer engine..."
    stop
    if [[ $? == 0 ]]; then
        exit 0
    else
        quit "Byzer engine is not running"
    fi
# restart command
elif [ "$1" == "restart" ]; then
    echo "Restarting Byzer engine..."
    echo "--> Stopping Byzer engine first if it's running..."
    stop
    if [[ $? != 0 ]]; then
        echo "    Byzer engine is not running, now start it"
    fi
    echo "--> Re-starting Byzer engine..."
    start
else
    quit "Usage: 'byzer.sh [-v] start' or 'byzer.sh [-v] stop' or 'byzer.sh [-v] restart'"
fi