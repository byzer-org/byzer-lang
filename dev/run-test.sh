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

set -u
set -e
set -o pipefail

DEV_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)

function exit_with_usage {
  cat << EOF
usage: run-test
Execute byzer's unit and integration testing.
Inputs are specified with the following environment variables:

MLSQL_SPARK_VERSION                - the spark version, 2.4/3.0 default 2.4
TEST_MODULES_FLAG all|it|ut        - type of test, default it
matches                            - Specify a regular expression for testfile, default .*
EOF
  exit 1
}

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

if [ -n "$1" ]; then
  export MLSQL_SPARK_VERSION=${1}
else
  export MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION:-3.0}
fi

TEST_MODULES_FLAG=${2:-black}
MATCHES=${3:-.*}

echo "Current parameters: $*"

if [ "${MLSQL_SPARK_VERSION}" == "3.0" ]; then
  SCALA_BINARY_VERSION=2.12
  if [ ! -f "${DEV_DIR}"/ansj_seg-5.1.6.jar ]; then
    wget --no-check-certificate --no-verbose "http://download.mlsql.tech/nlp/ansj_seg-5.1.6.jar" --directory-prefix "${DEV_DIR}/"
  fi
  if [ ! -f "${DEV_DIR}"/nlp-lang-1.7.8.jar ]; then
    wget --no-check-certificate --no-verbose http://download.mlsql.tech/nlp/nlp-lang-1.7.8.jar --directory-prefix "${DEV_DIR}/"
  fi
  echo "Download 3rd-party jars succeed"
  ./dev/make-distribution.sh

elif [ "${MLSQL_SPARK_VERSION}" == "2.4" ]; then
  SCALA_BINARY_VERSION=2.11
  ./dev/change-scala-version.sh 2.11
  python ./dev/python/convert_pom.py 2.4
else
  echo "Only accept 2.4|3.0"
  exit 1
fi

case "${TEST_MODULES_FLAG}" in
    all)     TEST_MODULES=-DargLine='"'-Dmatches=${MATCHES}'"';;
    it)   TEST_MODULES="-pl streamingpro-it -DargLine=-Dmatches=${MATCHES}";;
    ut)      args=(-Dtest.regex='"(streamingpro-it-'"${MLSQL_SPARK_VERSION}""_""${SCALA_BINARY_VERSION}"')"') && TEST_MODULES=${args[@]};;
    *)       echo "Only support all|it|ut" && exit 1
esac

echo test moules is :"${TEST_MODULES}"

if [ "${SKIP_INSTALL:-}" != "skipInstall" ]; then
  mvn clean install test $TEST_MODULES
else
  mvn clean test $TEST_MODULES
fi