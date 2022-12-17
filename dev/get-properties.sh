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

#if [ $# != 1 ]
#then
#    if [[ $# -lt 2 || $2 != 'DEC' ]]
#        then
#            echo 'invalid input'
#            exit 1
#    fi
#fi

if [ -z $BYZER_HOME ];then
    export BYZER_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

export SPARK_HOME=$BYZER_HOME/spark

byzer_tools_log4j="${BYZER_HOME}/conf/byzer-tools-log4j2.properties"

mkdir -p ${BYZER_HOME}/logs

result=$(${JAVA} -Dlog4j2.configurationFile=$byzer_tools_log4j -cp "${BYZER_HOME}/main/*" tech.mlsql.tool.ByzerConfigCLI $@ 2>>${BYZER_HOME}/logs/shell.stderr)

echo "$result"