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

# source me

function isValidJavaVersion() {
    version=$(${JAVA} -version 2>&1 | awk -F\" '/version/ {print $2}')
    version_first_part="$(echo ${version} | cut -d '.' -f1)"
    version_second_part="$(echo ${version} | cut -d '.' -f2)"
    if [[ "$version_first_part" -eq "1" ]] && [[ "$version_second_part" -eq "8" ]]; then
        # jdk version: 1.8.0-332
        echo "true"
        exit 0
    elif [[ "$version_first_part" -ge "8" ]]; then
        # jdk version: 11.0.15 / 17.0.3
        echo "true"
        exit 0
    else
        echo "false"
        exit 0
    fi
}

# avoid re-entering
if [[ "$dir" == "" ]]
then
    dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

    # misc functions
    function quit {
        echo "$@"
        if [[ -n "${QUIT_MESSAGE_LOG}" ]]; then
            echo $(setColor 31 "$@") >> ${QUIT_MESSAGE_LOG}
        fi
        exit 1
    }

    function verbose {
        if [[ -n "$verbose" ]]; then
            echo "$@"
        fi
    }

    function setColor() {
        echo -e "\033[$1m$2\033[0m"
    }

    function getValueByKey() {
        while read line
        do key=${line%=*} val=${line#*=}
        if [ "${key}" == "$1" ]; then
            echo $val
            break
        fi
        done<$2
    }

    # setup verbose
    verbose=${verbose:-""}
    while getopts ":v" opt; do
        case $opt in
            v)
                echo "Turn on verbose mode." >&2
                export verbose=true
                ;;
            \?)
                echo "Invalid option: -$OPTARG" >&2
                ;;
        esac
    done

    # check Machine
    unameOut="$(uname -s)"
    case "${unameOut}" in
        Linux*)     os=Linux;;
        Darwin*)    os=Mac;;
        CYGWIN*)    os=Cygwin;;
        MINGW*)     os=MinGw;;
        *)          os="UNKNOWN:${unameOut}"
    esac
    export MACHINE_OS=$os

    # set BYZER_HOME
    if [ -z $BYZER_HOME ];then
        export BYZER_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
    fi

   # set JAVA
   if [[ "${JAVA}" == "" ]]; then
       if [[ -z "$JAVA_HOME" ]]; then
           # if $JAVA_HOME is not found
           if [[ $MACHINE_OS == "Mac" ]]; then
               if  command -v java &> /dev/null ; then
               # try to use jdk in system
                   JAVA_HOME=$(dirname $(dirname $(readlink $(which java))))
               elif [[ -d "${BYZER_HOME}/jdk8/Contents/Home/" ]]; then
                   # No Java found, try to use embedded open jdk (only works on byzer-all-in-one)
                   JAVA_HOME=${BYZER_HOME}/jdk8/Contents/Home
               else
                   quit "Java environment not found, Java 1.8 or above is required."
               fi
           elif [[ $MACHINE_OS == "Linux" ]]; then
               if command -v java &> /dev/null ; then
               # try to use jdk in system
                   JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
               elif [[ -d "${BYZER_HOME}/jdk8/" ]]; then
                   # No Java found, try to use embedded open jdk (only works on byzer-all-in-one)
                   JAVA_HOME="${BYZER_HOME}"/jdk8
               else
                   quit "Java environment not found, Java 1.8 or above is required."
               fi
           else
               quit "Not suppported operating system:  $MACHINE_OS"
           fi

           [[ -z "$JAVA_HOME" ]] && quit "JAVA_HOME is not found, please set JAVA_HOME"
           export JAVA_HOME
       fi

       # check java command is found
       export JAVA=$JAVA_HOME/bin/java
       [[ -e "${JAVA}" ]] || quit "${JAVA} does not exist. Please set JAVA_HOME correctly."
       verbose "java is ${JAVA}"
   fi


    # set BYZER_IP
    if [ -z $BYZER_IP ];then
    export BYZER_IP=$(ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1' | head -n 1)
    fi

    # set BYZER_PORT
    export BYZER_LANG_PORT=$($BYZER_HOME/bin/get-properties.sh streaming.driver.port)

    if [[ -z ${BYZER_LANG_PORT} ]]; then
        export BYZER_LANG_PORT=9003
    fi

    # set ServerMode
    export BYZER_SERVER_MODE=$($BYZER_HOME/bin/get-properties.sh byzer.server.mode)
    if [[ -z ${BYZER_SERVER_MODE} ]]; then
        export BYZER_SERVER_MODE="server"
    fi
fi
