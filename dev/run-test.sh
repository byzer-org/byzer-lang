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
USAGE
      sh dev/run-test.sh [BYZER_SPARK_VERSION] [all | it | ut] [file regex matches rule]

DESCRIPTION
      Execute byzer's unit and integration testing.It supports tests of multiple spark versions, and can be executed
      separately for unit test or integration test according to parameter settings. The integration test can support
      the execution of a test script alone.

      positional arguments:

      BYZER_SPARK_VERSION [3.0 | 3.3]          - Specify the spark version supported by the test, default 3.3.
      TEST_MODULES_FLAG [all | it | ut]        - The parameter `all` here means to execute all tests; `ut` means to
                                                execute only unit tests excluding integration test cases; `it` means
                                                to execute only integration tests under the streamingpro-it module,
                                                not to execute unit test cases of other modules, default `it`.
      matches                                  - Specify a regular expression for testfile, default `.*` .

EXAMPLE
      If you need to execute integration tests of the spark3 version (which is the default behavior), you can use the
      following command:

      $ sh dev/run-test.sh 3.3 it

      Or if you want to execute spark2 version of unit tests, you can use the command:

      $ sh dev/run-test.sh 3.3 ut
EOF
  exit 1
}

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

if [ -n "$1" ]; then
  export MLSQL_SPARK_VERSION=${1}
else
  export MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION:-3.3}
fi
BYZER_SPARK_VERSION=$MLSQL_SPARK_VERSION

TEST_MODULES_FLAG=${2:-it}
MATCHES=${3:-.*}
echo "Current parameters: $*"

echo "BYZER_SPARK_VERSION ${BYZER_SPARK_VERSION}"
if [ "${BYZER_SPARK_VERSION}" == "3.0" ] || [ "${BYZER_SPARK_VERSION}" == "3.3" ]; then
  SCALA_BINARY_VERSION=2.12
  if [ ! -f "${DEV_DIR}"/ansj_seg-5.1.6.jar ]; then
    wget --no-check-certificate --no-verbose "http://download.mlsql.tech/nlp/ansj_seg-5.1.6.jar" --directory-prefix "${DEV_DIR}/"
  fi
  if [ ! -f "${DEV_DIR}"/nlp-lang-1.7.8.jar ]; then
    wget --no-check-certificate --no-verbose http://download.mlsql.tech/nlp/nlp-lang-1.7.8.jar --directory-prefix "${DEV_DIR}/"
  fi
  echo "Download 3rd-party jars succeed"

  # When we try to test the spark3 version, build a tar package through `make-distribution.sh`, which is mounted to
  # docker for integration testing. If the spark2 version is tested, it is not supported for now, so there is no need
  # to package it.
  if [ "${SKIP_INSTALL:-}" != "skipInstall" ]
  then
    case "${TEST_MODULES_FLAG}" in
        all)     ./dev/make-distribution.sh;;
        it)      ./dev/make-distribution.sh;;
        ut)      echo "Current spark version is ${BYZER_SPARK_VERSION}. No need to pack, skip it.";;
        *)       echo "Only support all|it|ut" && exit 1
    esac
  fi
else
  echo "Only accept 3.0|3.3"
  exit 1
fi



case "${TEST_MODULES_FLAG}" in
    all)     TEST_MODULES=-DargLine='"'-Dmatches=${MATCHES}'"';;
    it)
      BYZER_TEST_FILTER=${BYZER_TEST_FILTER:-"-Dsuites=tech.mlsql.it.SimpleQueryTestSuite,tech.mlsql.it.ByzerScriptTestSuite"}
      TEST_MODULES="-pl streamingpro-it ${BYZER_TEST_FILTER} -DargLine=-Dmatches=${MATCHES}";;
    ut)      args=(-Dtest.regex='"(streamingpro-it-'"${BYZER_SPARK_VERSION}""_""${SCALA_BINARY_VERSION}"')"') && TEST_MODULES=${args[@]};;
    *)       echo "Only support all|it|ut" && exit 1
esac
echo test moules is :"${TEST_MODULES}"

if [ "${SKIP_INSTALL:-}" != "skipInstall" ]; then
  mvn clean install -DskipTests
else
  mvn clean
fi
mvn test $TEST_MODULES