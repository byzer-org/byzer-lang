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
if [ "$1" == "-v" ]; then
    shift
fi

mkdir -p ${BYZER_HOME}/logs

export BYZER_CONFIG_FILE="${BYZER_HOME}/conf/byzer.properties"

# avoid re-entering
if [[ "$CHECKENV_ING" == "" ]]; then
    export CHECKENV_ING=true

    mkdir -p ${BYZER_HOME}/logs
    LOG=${BYZER_HOME}/logs/check-env.out
    ERRORS=${BYZER_HOME}/logs/check-env.error
    TITLE="#title"

        echo ""
        echo $(setColor 33 "Byzer-lang is checking installation environment, log is at ${LOG}")
        echo ""

        rm -rf ${BYZER_HOME}/logs/tmp
        rm -f ${ERRORS}
        touch ${ERRORS}

        export CHECKENV_REPORT_PFX=">   "
        export QUIT_MESSAGE_LOG=${ERRORS}


#        CHECK_FILES=
        CHECK_FILES=$(ls ${BYZER_HOME}/bin/check-*.sh)
        for f in ${CHECK_FILES[@]}
        do
            if [[ ! $f == *check-env.sh ]]; then
                echo $(getValueByKey ${TITLE} ${f})
                echo ""                                                                             >>${LOG}
                echo "============================================================================" >>${LOG}
                echo "Checking $(basename $f)"                                                      >>${LOG}
                echo "----------------------------------------------------------------------------" >>${LOG}
                bash $f >>${LOG} 2>&1
                rtn=$?
                if [[ $rtn == 0 ]]; then
                    echo "...................................................[$(setColor 32 PASS)]"
                elif [[ $rtn == 3 ]]; then
                    echo "...................................................[$(setColor 33 SKIP)]"
                elif [[ $rtn == 4 ]];then
                    echo "...................................................[$(setColor 33 WARN)]"
                    WARN_INFO=$(tail -n 3 ${LOG})
                    echo $(setColor 33 "WARNING:")
                    echo -e "$WARN_INFO"  | sed 's/^/    &/g'
                else
                    echo "...................................................[$(setColor 31 FAIL)]"
                    cat  ${ERRORS} >> ${LOG}
                    tail ${ERRORS}
                    echo $(setColor 33 "Full log is at: ${LOG}")
                    exit 1
                fi
            fi
        done
        echo ""
        cat ${LOG} | grep "^${CHECKENV_REPORT_PFX}"
        echo $(setColor 33 "Checking environment finished successfully. To check again, run 'bin/check-env.sh' manually.")
        echo ""

    export CHECKENV_ING=
fi